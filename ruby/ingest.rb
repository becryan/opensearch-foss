# ---------------------------------------------------------------------------
#  Satellite metadata ingest: adapters and normaliser, in one file.
#
#  Four public catalogues answer the same question in four different shapes.
#  This file turns all of them into one canonical document.
#
#    adapt_stac / adapt_cmr / adapt_copernicus
#        One per catalogue. They only PLUCK values into [raw][...] staging
#        fields. They do no cleaning, no parsing and no guessing.
#
#    normalise
#        The only place anything is cleaned. Mission names, dates, cloud cover
#        and four geometry encodings, for every source alike.
#
#    filter
#        Logstash's entry point. Picks the adapter from the event's tags, then
#        runs the normaliser.
#
#  Adding a catalogue means writing one more adapt_* function and one more
#  branch in filter. Nothing else changes.
# ---------------------------------------------------------------------------

require "digest"
require "time"

def adapt_stac(event, source)
  feature = event.get("features")
  return false unless feature.is_a?(Hash)

  props = feature["properties"] || {}

  event.set("[raw][id]",           feature["id"])
  event.set("[raw][collection]",   feature["collection"])
  event.set("[raw][title]",        props["title"] || feature["id"])
  event.set("[raw][platform]",     props["platform"])
  event.set("[raw][instrument]",   Array(props["instruments"]).first)
  event.set("[raw][datetime]",     props["datetime"] || props["start_datetime"])
  event.set("[raw][cloud_cover]",  props["eo:cloud_cover"])
  event.set("[raw][geometry_geojson]", feature["geometry"])

  # Free text the normaliser may fall back on when a field is missing.
  event.set("[raw][mission_hint]", [feature["id"], feature["collection"]].compact.join(" "))

  event.set("[@metadata][source]",        source)
  event.set("[@metadata][source_format]", "stac-item")

  # Kept for retrieval but NOT indexed - see the template. Two STAC APIs
  # disagree on the type of the same property name, so indexing these
  # verbatim is how you earn a mapping conflict.
  event.set("source_properties", props)

  event.remove("features")
  true
end

def adapt_cmr(event)
  entry = event.get("[feed][entry]")
  return false unless entry.is_a?(Hash)

  event.set("[raw][id]",          entry["id"])
  event.set("[raw][title]",       entry["title"] || entry["producer_granule_id"])
  event.set("[raw][collection]",  entry["collection_concept_id"])
  event.set("[raw][datetime]",    entry["time_start"])
  event.set("[raw][cloud_cover]", entry["cloud_cover"])

  # Deliberately NOT setting [raw][platform] - CMR granule results do not
  # carry one. The normaliser has to infer the mission from free text, and
  # the raw_platform field stays null so the "before" aggregation shows it.
  event.set("[raw][mission_hint]",
            [entry["dataset_id"], entry["producer_granule_id"], entry["title"]].compact.join(" "))

  event.set("[raw][polygons_latfirst]", entry["polygons"])
  event.set("[raw][boxes]",             entry["boxes"])

  event.set("[@metadata][source]",        "nasa-cmr")
  event.set("[@metadata][source_format]", "cmr-json")
  event.set("source_properties", entry)

  event.remove("feed")
  true
end

def adapt_copernicus(event)
  product = event.get("value")
  return false unless product.is_a?(Hash)

  # Flatten the {Name, Value} attribute list into something addressable.
  attrs = {}
  Array(product["Attributes"]).each do |a|
    attrs[a["Name"]] = a["Value"] if a.is_a?(Hash) && a["Name"]
  end

  event.set("[raw][id]",           product["Id"])
  event.set("[raw][title]",        product["Name"])
  event.set("[raw][mission_hint]", product["Name"])
  event.set("[raw][datetime]",     (product["ContentDate"] || {})["Start"])
  event.set("[raw][cloud_cover]",  attrs["cloudCover"])
  event.set("[raw][instrument]",   attrs["instrumentShortName"])

  # This API also offers a ready-made GeoFootprint GeoJSON object, but we take
  # the WKT on purpose: parsing WKT is the realistic case for older catalogues.
  event.set("[raw][geometry_wkt]", product["Footprint"])

  event.set("[@metadata][source]",        "copernicus-dataspace")
  event.set("[@metadata][source_format]", "odata")

  staged = product.reject { |k, _| k == "Attributes" }
  staged["attributes_flat"] = attrs
  event.set("source_properties", staged)

  event.remove("value")
  true
end

# Mission naming is the single worst offender across catalogues. "sentinel-2b",
# "Sentinel-2B", "S2B", and "S2B_MSIL1C_..." all mean the same satellite, and a
# terms aggregation treats them as four different ones.
PLATFORM_PATTERNS = [
  [/sentinel[-_\s]?1a|(?:\A|[^a-z0-9])s1a(?:[^a-z0-9]|\z)/, "sentinel-1a", "sentinel-1"],
  [/sentinel[-_\s]?1b|(?:\A|[^a-z0-9])s1b(?:[^a-z0-9]|\z)/, "sentinel-1b", "sentinel-1"],
  [/sentinel[-_\s]?2a|(?:\A|[^a-z0-9])s2a(?:[^a-z0-9]|\z)/, "sentinel-2a", "sentinel-2"],
  [/sentinel[-_\s]?2b|(?:\A|[^a-z0-9])s2b(?:[^a-z0-9]|\z)/, "sentinel-2b", "sentinel-2"],
  [/sentinel[-_\s]?2c|(?:\A|[^a-z0-9])s2c(?:[^a-z0-9]|\z)/, "sentinel-2c", "sentinel-2"],
  [/landsat[-_\s]?8|(?:\A|[^a-z0-9])lc08(?:[^a-z0-9]|\z)/,  "landsat-8",   "landsat"],
  [/landsat[-_\s]?9|(?:\A|[^a-z0-9])lc09(?:[^a-z0-9]|\z)/,  "landsat-9",   "landsat"],
].freeze

# Weaker signal: enough to place the constellation, not the individual satellite.
CONSTELLATION_PATTERNS = [
  [/hls\.s30|hlss30|sentinel[-_\s]?2/, "sentinel-2"],
  [/hls\.l30|hlsl30|landsat/,          "landsat"],
  [/sentinel[-_\s]?1/,                 "sentinel-1"],
].freeze

PROCESSING_LEVEL_PATTERNS = [
  [/msil2a|_l2a|\bl2a\b/, "l2a"],
  [/msil1c|_l1c|\bl1c\b/, "l1c"],
  [/hls\.s30|hls\.l30|hlss30|hlsl30/, "hls-surface-reflectance"],
  [/_ard_|\bard\b/, "ard"],
  [/grdh|_grd/, "grd"],
].freeze

# --- geometry decoders ------------------------------------------------------

def ring_from_wkt_text(txt)
  txt.split(",").map do |pair|
    lon, lat = pair.strip.split(/\s+/)
    [lon.to_f, lat.to_f]
  end
end

# geography'SRID=4326;POLYGON ((lon lat, lon lat, ...))'  ->  GeoJSON
def parse_wkt(wkt)
  return nil unless wkt.is_a?(String)
  s = wkt.strip
        .sub(/\Ageography'/i, "").sub(/'\z/, "")
        .sub(/\ASRID=\d+\s*;\s*/i, "")
        .strip

  if s =~ /\AMULTIPOLYGON\s*\((.*)\)\z/im
    # Simplification: treats each ((...)) group as a hole-free polygon.
    polys = $1.scan(/\(\s*\((.*?)\)\s*\)/m).map { |m| [ring_from_wkt_text(m[0])] }
    return polys.empty? ? nil : { "type" => "MultiPolygon", "coordinates" => polys }
  elsif s =~ /\APOLYGON\s*\((.*)\)\z/im
    rings = $1.scan(/\(([^()]*)\)/m).map { |m| ring_from_wkt_text(m[0]) }
    return rings.empty? ? nil : { "type" => "Polygon", "coordinates" => rings }
  elsif s =~ /\APOINT\s*\(([^()]*)\)\z/im
    lon, lat = $1.strip.split(/\s+/).map(&:to_f)
    return { "type" => "Point", "coordinates" => [lon, lat] }
  end
  nil
end

# CMR: [["-33.18 146.99 -32.95 147.06 ..."]] with LATITUDE FIRST.
def parse_cmr_polygons(polys)
  Array(polys).each do |p|
    txt = p.is_a?(Array) ? p.first : p
    next unless txt.is_a?(String)
    nums = txt.strip.split(/[\s,]+/).map(&:to_f)
    next if nums.length < 8 || nums.length.odd?
    ring = nums.each_slice(2).map { |lat, lon| [lon, lat] }   # <- the swap
    ring << ring.first.dup if ring.first != ring.last          # close the ring
    return { "type" => "Polygon", "coordinates" => [ring] }
  end
  nil
end

# CMR fallback: "south west north east" as one string.
def parse_cmr_boxes(boxes)
  txt = Array(boxes).first
  txt = txt.first if txt.is_a?(Array)
  return nil unless txt.is_a?(String)
  s, w, n, e = txt.strip.split(/[\s,]+/).map(&:to_f)
  return nil if [s, w, n, e].compact.length < 4
  { "type" => "Polygon", "coordinates" => [[[w, s], [e, s], [e, n], [w, n], [w, s]]] }
end

def valid_geojson?(g)
  g.is_a?(Hash) && g["type"].is_a?(String) && !g["coordinates"].nil?
end

def all_positions(geom)
  c = geom["coordinates"]
  case geom["type"]
  when "Point"           then [c]
  when "LineString"      then c
  when "Polygon"         then c.flatten(1)
  when "MultiPolygon"    then c.flatten(2)
  when "MultiLineString" then c.flatten(1)
  else []
  end
end

# OpenSearch geo_shape reads outer rings counterclockwise by default. A
# clockwise ring is legal GeoJSON but indexes as "everywhere except here",
# which is how a single scene ends up appearing to cover the planet.
def ensure_ccw(ring)
  return ring unless ring.is_a?(Array) && ring.length > 3
  area = 0.0
  (0...(ring.length - 1)).each do |i|
    area += (ring[i][0] * ring[i + 1][1]) - (ring[i + 1][0] * ring[i][1])
  end
  area < 0 ? ring.reverse : ring
end

def fix_winding(geom)
  case geom["type"]
  when "Polygon"
    geom["coordinates"] = geom["coordinates"].each_with_index.map { |r, i| i.zero? ? ensure_ccw(r) : r }
  when "MultiPolygon"
    geom["coordinates"] = geom["coordinates"].map do |poly|
      poly.each_with_index.map { |r, i| i.zero? ? ensure_ccw(r) : r }
    end
  end
  geom
end

# --- scalar decoders --------------------------------------------------------

def parse_datetime(value)
  return nil if value.nil?
  return Time.at(value.to_i).utc if value.is_a?(Numeric) && value.to_i > 100_000_000
  s = value.to_s.strip
  return nil if s.empty?
  # Slashes and a space instead of ISO-8601 T, seen in older inventory dumps.
  candidate = s.gsub("/", "-").sub(/\A(\d{4}-\d{2}-\d{2}) (\d{2}:)/, '\1T\2')
  begin
    Time.parse(candidate).utc
  rescue StandardError
    nil
  end
end

# Handles 12.5, "12.5", "12.5%", "" and nil alike.
def parse_cloud_cover(value)
  return nil if value.nil?
  return value.to_f if value.is_a?(Numeric)
  s = value.to_s.strip.delete("%").strip
  return nil if s.empty?
  return nil unless s =~ /\A-?\d+(\.\d+)?\z/
  s.to_f
end

def match_table(text, table)
  table.each { |row| return row if text =~ row[0] }
  nil
end

# --- main -------------------------------------------------------------------

def normalise(event)
  notes      = []
  quarantine = []

  event.set("source",        event.get("[@metadata][source]"))
  event.set("source_format", event.get("[@metadata][source_format]"))

  # --- identity ---
  scene_id = event.get("[raw][id]")
  if scene_id.nil? || scene_id.to_s.strip.empty?
    quarantine << "missing-id"
  else
    event.set("scene_id", scene_id.to_s)
  end
  event.set("title",      event.get("[raw][title]").to_s) unless event.get("[raw][title]").nil?
  event.set("collection", event.get("[raw][collection]").to_s) unless event.get("[raw][collection]").nil?

  # --- mission ---
  raw_platform = event.get("[raw][platform]")
  hint = [raw_platform, event.get("[raw][mission_hint]")].compact.join(" ").downcase

  if (hit = match_table(hint, PLATFORM_PATTERNS))
    event.set("platform",      hit[1])
    event.set("constellation", hit[2])
    notes << "platform-inferred-from-text" if raw_platform.nil?
    notes << "platform-renamed" if raw_platform && raw_platform.to_s.downcase != hit[1]
  elsif (hit = match_table(hint, CONSTELLATION_PATTERNS))
    # Genuinely unknowable at granule level, e.g. harmonised HLS products.
    event.set("constellation", hit[1])
    event.set("platform", "#{hit[1]}-unspecified")
    notes << "platform-not-resolvable-constellation-only"
  else
    event.set("platform",      "unknown")
    event.set("constellation", "unknown")
    notes << "platform-unresolved"
  end

  if (hit = match_table(hint, PROCESSING_LEVEL_PATTERNS))
    event.set("processing_level", hit[1])
  else
    event.set("processing_level", "unknown")
  end

  instrument = event.get("[raw][instrument]")
  event.set("instrument", instrument.to_s.downcase) unless instrument.nil?

  # --- acquisition time ---
  raw_dt = event.get("[raw][datetime]")
  parsed_dt = parse_datetime(raw_dt)
  if parsed_dt
    event.set("datetime", parsed_dt.strftime("%Y-%m-%dT%H:%M:%S.%LZ"))
  else
    quarantine << (raw_dt.nil? ? "missing-datetime" : "unparseable-datetime")
  end

  # --- cloud cover ---
  raw_cc = event.get("[raw][cloud_cover]")
  cc = parse_cloud_cover(raw_cc)
  if cc.nil?
    notes << "cloud-cover-absent" if raw_cc.nil?
    notes << "cloud-cover-unparseable" unless raw_cc.nil?
  else
    notes << "cloud-cover-coerced-from-string" if raw_cc.is_a?(String)
    if cc < 0 || cc > 100
      notes << "cloud-cover-out-of-range-dropped"
    else
      event.set("cloud_cover", cc)
    end
  end

  # --- geometry: four encodings in, one out ---
  geom     = nil
  encoding = "none"

  if valid_geojson?(event.get("[raw][geometry_geojson]"))
    geom     = event.get("[raw][geometry_geojson]")
    encoding = "geojson"
  elsif (g = parse_wkt(event.get("[raw][geometry_wkt]")))
    geom     = g
    encoding = "wkt"
    notes << "geometry-parsed-from-wkt"
  elsif (g = parse_cmr_polygons(event.get("[raw][polygons_latfirst]")))
    geom     = g
    encoding = "lat-first-pairs"
    notes << "geometry-coordinates-reordered-to-lon-lat"
  elsif (g = parse_cmr_boxes(event.get("[raw][boxes]")))
    geom     = g
    encoding = "bbox-string"
    notes << "geometry-widened-to-bbox"
  end

  event.set("raw_geometry_encoding", encoding)

  if geom.nil?
    quarantine << "missing-geometry"
  else
    positions = all_positions(geom).select { |p| p.is_a?(Array) && p.length >= 2 }
    if positions.empty?
      quarantine << "empty-geometry"
    else
      lons = positions.map { |p| p[0].to_f }
      lats = positions.map { |p| p[1].to_f }
      w, e = lons.min, lons.max
      s, n = lats.min, lats.max

      if lats.any? { |v| v < -90 || v > 90 } || lons.any? { |v| v < -180 || v > 180 }
        quarantine << "coordinates-out-of-range"
      else
        before = geom["coordinates"].to_s
        geom   = fix_winding(geom)
        notes << "polygon-rewound-counterclockwise" if geom["coordinates"].to_s != before

        # A footprint wider than 180 degrees is almost always a scene that
        # crosses the antimeridian and was written as if it did not.
        notes << "suspected-antimeridian-crossing" if (e - w) > 180

        event.set("geometry", geom)
        event.set("bbox", [w, s, e, n])
        event.set("centroid", { "lat" => (s + n) / 2.0, "lon" => (w + e) / 2.0 })
      end
    end
  end

  # --- keep the messy originals alongside the clean values, so the talk can
  #     aggregate on both and show the difference in one screen ---
  event.set("raw_platform",    raw_platform.to_s) unless raw_platform.nil?
  event.set("raw_datetime",    raw_dt.to_s)       unless raw_dt.nil?
  event.set("raw_cloud_cover", raw_cc.to_s)       unless raw_cc.nil?

  event.set("ingest_notes", notes.uniq)
  event.remove("raw")

  if quarantine.empty?
    event.set("[@metadata][target_index]", "satellite-metadata")
  else
    event.set("quarantine_reasons", quarantine.uniq)
    event.set("[@metadata][target_index]", "satellite-metadata-quarantine")
  end

  # A stable document id makes the whole pipeline idempotent: polling the same
  # catalogue again updates documents in place instead of inflating the counts,
  # which matters when you re-run the demo a few times before going on stage.
  src = event.get("source").to_s
  if event.get("scene_id")
    event.set("[@metadata][doc_id]", "#{src}:#{event.get('scene_id')}")
  else
    event.set("[@metadata][doc_id]",
              "#{src}:sha1:#{Digest::SHA1.hexdigest(event.get('source_properties').to_s)}")
  end

  true
end

# ---------------------------------------------------------------------------
# Logstash entry point. The pipeline tags each event with its source, so the
# dispatch happens here rather than needing a separate ruby filter per
# catalogue in pipeline.conf.
# ---------------------------------------------------------------------------

def filter(event)
  tags = Array(event.get("tags"))

  adapted =
    if tags.include?("src_dea")
      adapt_stac(event, "ga-dea-stac")
    elsif tags.include?("src_earthsearch")
      adapt_stac(event, "element84-earth-search")
    elsif tags.include?("src_cmr")
      adapt_cmr(event)
    elsif tags.include?("src_copernicus")
      adapt_copernicus(event)
    else
      false
    end

  # Drop anything an adapter could not make sense of, rather than passing a
  # half-populated event to the normaliser.
  return [] unless adapted

  normalise(event)
  [event]
end
