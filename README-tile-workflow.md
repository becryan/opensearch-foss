# The tiled workflow in detail

Quick start is in [README.md](README.md). 

## What the workflow does

`workflow/tile_worker.py` is an external application that points at OpenSearch
and does two things a real processing pipeline might do:

1. Asks OpenSearch where the work is. It lays a grid over the area of interest
   and counts, in one `filters` aggregation with a `geo_shape` filter per tile,
   how many indexed scenes intersect each tile.
2. Writes one document per tile and updates it in place as the tile moves from
   pending to running to complete or failed, so a dashboard can show the grid
   filling in while the job runs.

The default grid is 1 degree tiles over south-east Australia, which is 143
tiles. Tiles with no scenes are marked `skipped` rather than left pending
forever. A run takes a couple of minutes at the default rate.

Options worth knowing: `--tile-size` in degrees, `--bbox west,south,east,north`,
`--rate` tiles per second, `--run-id` to keep several runs side by side, and
`--seed-value` so a rehearsal repeats exactly.

    workflow/tile_worker.py                     the workflow itself
    templates/tile-workflow-template.json       mapping for tile progress
    templates/tile-workflow-logs-template.json  mapping for the event log
    dashboards/tile-workflow.ndjson             the dashboard, as a file

## Watching it happen

Open the tile dashboard, which restores a
relative time range and auto-refreshes every 5 seconds:

    http://localhost:5601/app/dashboards#/view/tile-workflow-overview

then run `./workflow/tile_worker.py run` in a terminal beside it. Tiles turn
green as they land.

Panels: tile count, fraction complete, status donut, a failed-tile table, the
completion grid, a throughput chart on `finished_at`, the tile map, a failure
causes table, and the log itself.

## Two ways the same completion is drawn

The **completion grid** is a heat map whose axes are the tile's longitude and
latitude. It reads as a map, but because it is a plain chart it needs no basemap
tiles, always frames itself on the data, and never opens at the wrong zoom. This
is the panel to demo on venue wifi.

The **tile map** is the real thing: actual tile polygons as `geo_shape`, with one
layer per status so complete is green, failed red, running amber, pending grey
and skipped pale. It opens over south-east Australia thanks to the patched image
described under [Making a map open zoomed in](#making-a-map-open-zoomed-in).

## The log

The worker writes an event log to `tile-workflow-logs`, separate from
`tile-workflow`. The two indices earn their separation: `tile-workflow` is
current state, one document per tile, updated in place; the log is append-only,
one document per thing that happened.

The dashboard shows it through a saved search, so it reads like a log rather
than a chart. Type `level: ERROR` in the dashboard query bar to cut it down to
failures. A typical run produces a few hundred lines across INFO, WARN and
ERROR.

Turn it off with `--no-logs`. `reset` clears the log along with the tiles.


## Making a map open zoomed in

`Dockerfile.dashboards` patches a zoom-level constant in the built browser bundle, which
is why the maps open over south-east Australia at zoom 3. To change it, edit the
`ARG MAP_ZOOM` default or override it for one build:

    docker compose build --build-arg MAP_ZOOM=4 opensearch-dashboards
    docker compose up -d opensearch-dashboards

This is a patch of vendored code (opensearch dashboards v 2.18.0) and should check 
upgrades.  To go back to stock version, swap `build:` for
`image: opensearchproject/opensearch-dashboards:2.18.0` in `docker-compose.yml`.

The base map tiles come from `tiles.maps.opensearch.org` and
`maps.opensearch.org`, so the map panels need internet even though the data does
not. Without it the tiles still draw, just on a blank background.


## Resetting

    docker compose down                  # keep the indexed data
    docker compose down -v               # throw the data away too

To re-run just the workflow without touching the scene index:

    ./workflow/tile_worker.py reset
    ./workflow/tile_worker.py seed
    ./workflow/tile_worker.py run
