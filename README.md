# OpenSearch - demo'd at FOSS4G Oceanea 2025
OpenSearch is a platform for storing has geospatial capability

A single-node OpenSearch stack demonstrating a **tiled geospatial workflow**: an
external application processes tiles over an area of interest and reports its
progress into OpenSearch, so you can watch completion fill in on a dashboard.

Docker is the only prerequisite. The worker is standard library Python.

## Quick start

**1. Set an admin password**, strong enough for
[zxcvbn](https://github.com/dropbox/zxcvbn).

    cp example.env .env
    # then edit .env

**2. Start the stack.** Takes a couple of minutes: it ingests the satellite
scene metadata the workflow counts against, and imports the dashboards.

    docker compose up -d
    ./demo/setup.sh

**3. Open the dashboard**, `admin` and your `.env` password. It auto-refreshes
every 5 seconds.

    http://localhost:5601/app/dashboards#/view/tile-workflow-overview

**4. Run the workflow beside it** and watch the tiles turn green.

    ./workflow/tile_worker.py seed
    ./workflow/tile_worker.py run

## Other commands

    ./workflow/tile_worker.py status    # text summary
    ./workflow/tile_worker.py retry     # requeue the failures
    ./workflow/tile_worker.py reset     # start over

About 8 percent of tiles fail on purpose, so the failure table, the log and the
retry path all have something in them. `--fail-rate 0` for a clean run.

If nothing appears, check `docker compose ps` and re-run `./demo/setup.sh`,
which is safe to repeat. Step 2 needs network access because the scene
catalogues are live public APIs; everything after that works offline.

## Docs

- [README-tile-workflow.md](README-tile-workflow.md) - how it works, the
  dashboard panels, editing the dashboards, and the traps.
- [README-satellite-metadata.md](README-satellite-metadata.md) - the second
  example, which produces the scene data step 2 ingests.
- [links-FOSS.md](links-FOSS.md) - links from the talk.
