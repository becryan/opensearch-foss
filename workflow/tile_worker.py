#!/usr/bin/env python3
"""A tiled geospatial workflow that reports its progress to OpenSearch.

This is the "external application" half of the demo. It does two things that a
real processing pipeline does:

  1. Asks OpenSearch what work exists, by counting indexed scenes that
     intersect each tile of a grid over the area of interest.
  2. Writes one document per tile and updates it as the tile moves through
     pending -> running -> complete (or failed), so a dashboard can show the
     grid filling in while the job runs.

Standard library only, so there is nothing to install.

    ./workflow/tile_worker.py seed          # build the grid, count real scenes
    ./workflow/tile_worker.py run           # process tiles, updating as it goes
    ./workflow/tile_worker.py status        # print a text summary
    ./workflow/tile_worker.py reset         # delete the run

Watch it happen: open the Tile workflow dashboard, set auto-refresh to 5
seconds, and run "run" in a terminal beside it.
"""

import argparse
import base64
import json
import os
import random
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

INDEX = "tile-workflow"
LOGS_INDEX = "tile-workflow-logs"
SCENES_INDEX = "satellite-metadata"
_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "templates")
TEMPLATES = {
    INDEX: os.path.join(_TEMPLATE_DIR, "tile-workflow-template.json"),
    LOGS_INDEX: os.path.join(_TEMPLATE_DIR, "tile-workflow-logs-template.json"),
}

# South-east Australia, matching the area the Logstash pipeline harvests.
DEFAULT_BBOX = (141.0, -39.0, 154.0, -28.0)   # west, south, east, north

STATUS_CODES = {"pending": 0, "running": 1, "complete": 2, "failed": 3}

# The kinds of thing that actually go wrong in a tiled raster job. error_type is
# the stable code to aggregate on; the message is the human sentence, which
# embeds a path or a number and so makes a poor aggregation key.
FAILURES = [
    ("source_asset_unreadable",
     "HTTP 404 fetching s3://sentinel-cogs/{tile}/B04.tif",
     "3 of 12 assets missing; the granule may have been reprocessed upstream"),
    ("reprojection_failed",
     "gdalwarp could not compute output bounds for EPSG:32755 to EPSG:3577",
     "source has no geotransform and no ground control points"),
    ("worker_out_of_memory",
     "worker killed: resident set 4.2 GiB exceeded the 4.0 GiB limit",
     "tile holds {scenes} scenes; consider a smaller tile or streaming reads"),
    ("checksum_mismatch",
     "MD5 mismatch on downloaded asset for tile {tile}",
     "expected 265e47eccfb7f08c, got 9f1a02b4c7de5510"),
    ("upstream_timeout",
     "tile exceeded its 300 second budget and was cancelled",
     "last log line was 'reading band 8A' 291 seconds in"),
]

# Not everything worth logging is a failure. These keep the log stream
# realistic rather than a wall of identical INFO lines.
WARNINGS = [
    "cloud cover 93% exceeds the 80% threshold, continuing anyway",
    "2 of {scenes} scenes have no thumbnail asset",
    "scene footprint only partially covers the tile, edges will be nodata",
    "retrying asset fetch after a 503 from the source catalogue",
]


# --------------------------------------------------------------------------
# OpenSearch access. Basic auth over the demo certificates, so verification is
# off. Fine on a laptop, not fine anywhere else.
# --------------------------------------------------------------------------

class OpenSearch:
    def __init__(self, url, user, password):
        self.url = url.rstrip("/")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ctx))
        # Sent up front rather than relying on a 401 challenge.
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.opener.addheaders = [("Authorization", f"Basic {token}")]

    def request(self, method, path, body=None, ndjson=False):
        data = None
        ctype = "application/json"
        if body is not None:
            if ndjson:
                data = body.encode()
                ctype = "application/x-ndjson"
            else:
                data = json.dumps(body).encode()
        req = urllib.request.Request(f"{self.url}{path}", data=data, method=method)
        req.add_header("Content-Type", ctype)
        try:
            with self.opener.open(req, timeout=120) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:600]
            raise SystemExit(f"OpenSearch {method} {path} -> HTTP {e.code}\n{detail}")
        except urllib.error.URLError as e:
            raise SystemExit(f"Cannot reach OpenSearch at {self.url}: {e.reason}\n"
                             "Is the stack up?  docker compose up -d")


def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# --------------------------------------------------------------------------
# The grid
# --------------------------------------------------------------------------

def build_grid(bbox, size):
    """Regular lon/lat tiles over the bbox. Each tile keeps its south-west
    corner, which becomes the axis value on the completion heat map."""
    west, south, east, north = bbox
    tiles = []
    lat = south
    while lat < north - 1e-9:
        lon = west
        while lon < east - 1e-9:
            w, s = round(lon, 6), round(lat, 6)
            e, n = round(min(lon + size, east), 6), round(min(lat + size, north), 6)
            tiles.append({
                "tile_id": f"{'E' if w >= 0 else 'W'}{abs(w):g}_{'S' if s < 0 else 'N'}{abs(s):g}",
                "tile_lon": int(w),
                "tile_lat": int(s),
                "tile_size_deg": size,
                # Counterclockwise, so OpenSearch does not read the polygon as
                # covering everything except the tile.
                "geometry": {"type": "Polygon",
                             "coordinates": [[[w, s], [e, s], [e, n], [w, n], [w, s]]]},
                "centroid": {"lat": (s + n) / 2.0, "lon": (w + e) / 2.0},
                "bounds": (w, s, e, n),
            })
            lon += size
        lat += size
    return tiles


def count_scenes_per_tile(os_client, tiles):
    """One request, not one per tile: a filters aggregation with a geo_shape
    filter per tile. This is the workflow asking OpenSearch where the work is."""
    filters = {}
    for t in tiles:
        w, s, e, n = t["bounds"]
        filters[t["tile_id"]] = {
            "geo_shape": {
                "geometry": {
                    # envelope is [top-left, bottom-right]
                    "shape": {"type": "envelope", "coordinates": [[w, n], [e, s]]},
                    "relation": "intersects",
                }
            }
        }
    body = {"size": 0, "aggs": {"per_tile": {"filters": {"filters": filters}}}}
    res = os_client.request("POST", f"/{SCENES_INDEX}/_search", body)
    buckets = res.get("aggregations", {}).get("per_tile", {}).get("buckets", {})
    return {k: v["doc_count"] for k, v in buckets.items()}


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------

def ensure_templates(os_client):
    for name, path in TEMPLATES.items():
        with open(path) as f:
            os_client.request("PUT", f"/_index_template/{name}", json.load(f))


class Log:
    """Buffers log lines and bulk-writes them, so a long run does not make one
    request per line but the dashboard still updates while it is running."""

    def __init__(self, os_client, run_id, worker, enabled=True, flush_every=20):
        self.os, self.run_id, self.worker = os_client, run_id, worker
        self.enabled, self.flush_every = enabled, flush_every
        self.buf = []

    def emit(self, level, event, message, **fields):
        if not self.enabled:
            return
        doc = {"@timestamp": iso(now()), "run_id": self.run_id,
               "worker": self.worker, "level": level, "event": event,
               "message": message}
        doc.update({k: v for k, v in fields.items() if v is not None})
        self.buf.append(json.dumps({"index": {"_index": LOGS_INDEX}}) + "\n")
        self.buf.append(json.dumps(doc) + "\n")
        if len(self.buf) >= self.flush_every * 2:
            self.flush()

    def flush(self):
        if self.buf:
            bulk(self.os, self.buf)
            self.buf = []


def doc_id(run_id, tile_id):
    # Stable, so progress updates overwrite rather than accumulate.
    return f"{run_id}:{tile_id}"


def bulk(os_client, lines):
    if not lines:
        return
    payload = "".join(lines)
    res = os_client.request("POST", "/_bulk?refresh=wait_for", payload, ndjson=True)
    if res.get("errors"):
        first = next((i for i in res["items"] if list(i.values())[0].get("error")), None)
        raise SystemExit(f"bulk indexing failed: {json.dumps(first)[:500]}")


def cmd_seed(os_client, args):
    ensure_templates(os_client)
    tiles = build_grid(args.bbox, args.tile_size)
    print(f"grid: {len(tiles)} tiles of {args.tile_size} degrees over {args.bbox}")

    counts = count_scenes_per_tile(os_client, tiles)
    with_work = sum(1 for t in tiles if counts.get(t["tile_id"], 0) > 0)
    # Sum of per-tile counts, so a scene spanning two tiles counts in both.
    print(f"scene-tile intersections found by OpenSearch: {sum(counts.values())} "
          f"across {with_work} tiles")

    queued = iso(now())
    lines = []
    for t in tiles:
        n = counts.get(t["tile_id"], 0)
        # A tile with no scenes has nothing to do. Marking it skipped is more
        # honest than leaving it pending forever.
        status = "pending" if n > 0 else "skipped"
        doc = {
            "@timestamp": queued,
            "run_id": args.run_id,
            "tile_id": t["tile_id"],
            "tile_lon": t["tile_lon"],
            "tile_lat": t["tile_lat"],
            "tile_size_deg": t["tile_size_deg"],
            "geometry": t["geometry"],
            "centroid": t["centroid"],
            "status": status,
            "status_code": STATUS_CODES.get(status, 0),
            "is_complete": 0,
            "scenes_in_tile": n,
            "queued_at": queued,
            "attempts": 0,
            "worker": args.worker,
        }
        lines.append(json.dumps({"index": {"_index": INDEX,
                                           "_id": doc_id(args.run_id, t["tile_id"])}}) + "\n")
        lines.append(json.dumps(doc) + "\n")
    bulk(os_client, lines)
    log = Log(os_client, args.run_id, args.worker, enabled=not args.no_logs)
    log.emit("INFO", "run_seeded",
             f"seeded {len(tiles)} tiles of {args.tile_size} degrees; "
             f"{with_work} have scenes, {len(tiles) - with_work} skipped")
    log.flush()
    print(f"seeded run '{args.run_id}': {with_work} tiles to do, "
          f"{len(tiles) - with_work} skipped (no scenes)")


def fetch_pending(os_client, run_id):
    body = {
        "size": 2000,
        "_source": ["tile_id", "scenes_in_tile", "attempts"],
        "query": {"bool": {"filter": [{"term": {"run_id": run_id}},
                                      {"term": {"status": "pending"}}]}},
        "sort": [{"tile_lat": "asc"}, {"tile_lon": "asc"}],
    }
    res = os_client.request("POST", f"/{INDEX}/_search", body)
    return [h["_source"] for h in res["hits"]["hits"]]


def update(os_client, run_id, tile_id, patch):
    os_client.request("POST", f"/{INDEX}/_update/{doc_id(run_id, tile_id)}",
                      {"doc": patch})


def cmd_run(os_client, args):
    todo = fetch_pending(os_client, args.run_id)
    if not todo:
        print(f"nothing pending in run '{args.run_id}'. Seed it first:")
        print(f"  ./workflow/tile_worker.py seed --run-id {args.run_id}")
        return

    print(f"processing {len(todo)} tiles at ~{args.rate}/s "
          f"(failure rate {args.fail_rate:.0%})")
    rng = random.Random(args.seed_value)
    log = Log(os_client, args.run_id, args.worker, enabled=not args.no_logs)
    log.emit("INFO", "run_started",
             f"run started over {len(todo)} pending tiles")
    done = failed = 0

    for i, tile in enumerate(todo, start=1):
        tid = tile["tile_id"]
        scenes = tile.get("scenes_in_tile", 0)
        attempt = tile.get("attempts", 0) + 1
        started = now()
        update(os_client, args.run_id, tid, {
            "status": "running", "status_code": STATUS_CODES["running"],
            "started_at": iso(started), "@timestamp": iso(started),
            "attempts": attempt, "worker": args.worker,
        })
        log.emit("INFO", "tile_started",
                 f"processing {scenes} scenes intersecting tile {tid}",
                 tile_id=tid, attempt=attempt, scenes_in_tile=scenes)

        # A warning now and then, so the log is not a wall of identical lines.
        if rng.random() < 0.18:
            log.emit("WARN", "tile_warning",
                     rng.choice(WARNINGS).format(scenes=scenes, tile=tid),
                     tile_id=tid, attempt=attempt, scenes_in_tile=scenes)

        # Stand-in for the actual per-tile processing. Bigger tiles take longer,
        # which is usually true and makes the map fill in unevenly.
        work = (1.0 / max(args.rate, 0.01)) * (0.5 + min(scenes, 20) / 20.0)
        time.sleep(work)

        finished = now()
        elapsed = int((finished - started).total_seconds() * 1000)
        patch = {
            "finished_at": iso(finished), "@timestamp": iso(finished),
            "duration_ms": elapsed,
        }
        if rng.random() < args.fail_rate:
            code, message, detail = rng.choice(FAILURES)
            message = message.format(tile=tid, scenes=scenes)
            detail = detail.format(tile=tid, scenes=scenes)
            patch.update({"status": "failed", "status_code": STATUS_CODES["failed"],
                          "is_complete": 0, "error": code})
            log.emit("ERROR", "tile_failed", message, tile_id=tid,
                     error_type=code, detail=detail, attempt=attempt,
                     duration_ms=elapsed, scenes_in_tile=scenes)
            failed += 1
        else:
            patch.update({"status": "complete", "status_code": STATUS_CODES["complete"],
                          "is_complete": 1})
            log.emit("INFO", "tile_complete",
                     f"tile {tid} written in {elapsed} ms", tile_id=tid,
                     attempt=attempt, duration_ms=elapsed, scenes_in_tile=scenes)
            done += 1
        update(os_client, args.run_id, tid, patch)

        if i % 10 == 0 or i == len(todo):
            print(f"  {i}/{len(todo)}  complete={done} failed={failed}")

    log.emit("ERROR" if failed else "INFO", "run_finished",
             f"run finished: {done} complete, {failed} failed")
    log.flush()

    print(f"run '{args.run_id}' finished: {done} complete, {failed} failed")
    if failed:
        print("  retry just the failures:")
        print(f"    ./workflow/tile_worker.py retry --run-id {args.run_id}")


def cmd_retry(os_client, args):
    body = {"query": {"bool": {"filter": [{"term": {"run_id": args.run_id}},
                                          {"term": {"status": "failed"}}]}},
            "script": {"source": "ctx._source.status='pending'; "
                                 "ctx._source.status_code=0; "
                                 "ctx._source.error=null"}}
    res = os_client.request("POST", f"/{INDEX}/_update_by_query?refresh=true", body)
    n = res.get("updated", 0)
    log = Log(os_client, args.run_id, args.worker, enabled=not args.no_logs)
    log.emit("WARN", "retry_requeued", f"requeued {n} failed tiles as pending")
    log.flush()
    print(f"requeued {n} failed tiles as pending")


def cmd_status(os_client, args):
    body = {
        "size": 0,
        "query": {"term": {"run_id": args.run_id}},
        "aggs": {
            "by_status": {"terms": {"field": "status", "size": 10}},
            "scenes": {"sum": {"field": "scenes_in_tile"}},
            "duration": {"stats": {"field": "duration_ms"}},
        },
    }
    res = os_client.request("POST", f"/{INDEX}/_search", body)
    total = res["hits"]["total"]["value"]
    if not total:
        print(f"run '{args.run_id}' has no tiles. Seed it first.")
        return
    aggs = res["aggregations"]
    counts = {b["key"]: b["doc_count"] for b in aggs["by_status"]["buckets"]}
    workable = total - counts.get("skipped", 0)
    complete = counts.get("complete", 0)
    pct = (complete / workable * 100) if workable else 0.0

    print(f"run '{args.run_id}'")
    print(f"  tiles           : {total} ({workable} with scenes to process)")
    for k in ("complete", "running", "pending", "failed", "skipped"):
        if k in counts:
            print(f"    {k:<10} {counts[k]:>5}")
    print(f"  completion      : {pct:.1f}% of workable tiles")
    print(f"  scene-tile hits : {int(aggs['scenes']['value'])} "
          f"(a scene spanning tiles counts in each)")
    d = aggs["duration"]
    if d.get("count"):
        print(f"  per-tile ms     : avg {d['avg']:.0f}  min {d['min']:.0f}  max {d['max']:.0f}")


def cmd_reset(os_client, args):
    body = {"query": {"term": {"run_id": args.run_id}}}
    res = os_client.request("POST", f"/{INDEX}/_delete_by_query?refresh=true", body)
    print(f"deleted {res.get('deleted', 0)} tile documents from run '{args.run_id}'")
    logs = os_client.request(
        "POST", f"/{LOGS_INDEX}/_delete_by_query?refresh=true&ignore_unavailable=true",
        body)
    print(f"deleted {logs.get('deleted', 0)} log lines from run '{args.run_id}'")


# --------------------------------------------------------------------------

def load_env():
    """Reads .env the same way the shell scripts do."""
    pw = os.environ.get("OPENSEARCH_INITIAL_ADMIN_PASSWORD")
    if pw:
        return pw
    path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line.startswith("OPENSEARCH_INITIAL_ADMIN_PASSWORD="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("Set OPENSEARCH_INITIAL_ADMIN_PASSWORD or create a .env file "
                     "(see example.env)")


def bbox_arg(text):
    parts = [float(x) for x in text.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("bbox must be west,south,east,north")
    return tuple(parts)


def main(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["seed", "run", "retry", "status", "reset"])
    p.add_argument("--url", default=os.environ.get("OPENSEARCH_URL",
                                                   "https://localhost:9200"))
    p.add_argument("--user", default="admin")
    p.add_argument("--run-id", default="run-1")
    p.add_argument("--worker", default="worker-a")
    p.add_argument("--bbox", type=bbox_arg, default=DEFAULT_BBOX,
                   help="west,south,east,north (default south-east Australia)")
    p.add_argument("--tile-size", type=float, default=1.0,
                   help="tile edge in degrees (default 1.0)")
    p.add_argument("--rate", type=float, default=2.0,
                   help="tiles per second, roughly (default 2)")
    p.add_argument("--fail-rate", type=float, default=0.08,
                   help="fraction of tiles that fail, to exercise the retry path")
    p.add_argument("--seed-value", type=int, default=7,
                   help="random seed, so a rehearsal is repeatable")
    p.add_argument("--no-logs", action="store_true",
                   help="do not write log lines to " + LOGS_INDEX)
    args = p.parse_args(argv)

    client = OpenSearch(args.url, args.user, load_env())
    {"seed": cmd_seed, "run": cmd_run, "retry": cmd_retry,
     "status": cmd_status, "reset": cmd_reset}[args.command](client, args)


if __name__ == "__main__":
    main(sys.argv[1:])
