# StormTrace

**An open, research-grade lakehouse-style pipeline that measures how space weather degrades the reliability of public satellite orbit data.**

StormTrace ingests public orbital elements (CelesTrak) and real-time space
weather (NOAA SWPC), preserves immutable point-in-time snapshots, derives
research features with SQL, and produces an explainable **Orbit Reliability
Index** — a trust signal for public orbit data, explicitly *not* a collision
warning system.

## Research Question

> Do geomagnetic disturbances produce measurable, correlated degradation in
> public orbit estimates across populations of LEO satellites, and can that
> degradation be detected from public data alone?

## Architecture

```text
CelesTrak GP (stations, cubesat, science, iridium-33-debris)
CelesTrak SW (daily Kp/Ap/F10.7)      NOAA SWPC RTSW      SatNOGS network
        |                                     |                 |
        v                                     v                 v
  ingest_celestrak.py                  ingest_noaa.py    ingest_satnogs.py
  ingest_space_weather.py
        |                                     |                 |
        +--------------- immutable Bronze ---------------------+
                        |
                load_history.py          (idempotent, checksummed)
                        |
                   DuckDB history
                        |
                check_quality.py        (gate: errors stop the pipeline)
                        |
     +----------------+----------------+
     |                |                |
summarize_history  build_gold    build_orbit_features
     |                |                |
     +----------------+----------------+
                      |
              analyze_research.py      (orbit change, freshness, ORI, charts)
                      |
        build_propagation_disagreement.py  (SGP4 drift between element sets)
                      |
              validate_ori.py         (point-in-time ORI backtest, Spearman)
                      |
        data\gold\*.csv  +  data\reports\*.png / research_summary.md
```

Orchestration: `run_pipeline.py` (lock-protected, policy-aware) or the
registered Windows scheduled task `StormTracePipeline` (hourly, with
two-hour per-source rate limiting). Each run ends by syncing all zones
into the MinIO lakehouse and publishing its events to the Redpanda stream
when those services are running (`docker compose up -d`). The status API
exposes `/metrics` for the Prometheus + Grafana monitoring stack.

## Quick Start

Requires Python 3.10+ on Windows.

```powershell
pip install -r requirements.txt
python src\run_pipeline.py --dry-run   # see what is due, change nothing
python src\run_pipeline.py             # collect, validate, build, analyze
```

Outputs land in `data\gold\` (CSV tables) and `data\reports\` (charts and
`research_summary.md`).

## Current Findings (from real collected data)

- **Source update frequency ≠ data change frequency**: consecutive snapshots
  can contain byte-identical element sets; measured "zero drag" was correctly
  reported as republished data, not as a physical measurement.
- **Real propagation disagreement measured**: across 290 refreshed element
  pairs (median span 9.6 h), the median SGP4 disagreement is 0.29 km, and
  the RIC decomposition separates drag (along-track dominant, e.g. SWIFT)
  from maneuvers (radial-dominant, e.g. the MMS formation) and from
  long-span SGP4 model error on elliptical orbits (CXO, 281 km over 63 h).
- **The Orbit Reliability Index is directionally validated**: across 290
  refreshed element pairs, median measured error rises monotonically across
  the moderate → reduced → low predicted classes, and Spearman
  score-vs-error strengthens as pairs accumulate (−0.36 → −0.43 → −0.49),
  with element age now correlating with error at +0.41 in the designed
  direction. The "high" class is contaminated by maneuvering spacecraft —
  an unmodeled failure mode documented in the report.
- **A learned model beats the hand-crafted index and finds a missing
  term**: a gradient-boosted model trained on point-in-time features
  predicts disagreement rate with pooled Spearman 0.565 (out-of-fold,
  grouped by object), and its strongest feature is **eccentricity**
  (importance 0.49) — a signal the index does not use. SGP4 error grows
  fastest on elliptical orbits (the MMS/CXO signature), so the index's
  next revision should include an eccentricity term.
- **Element freshness varies by object class**: median public element age
  is 9-18 hours depending on group; science satellites are tracked best,
  and roughly a quarter of cubesats carry elements older than 24 hours.
- **Low altitude dominates unreliability**: the ISS scores only "reduced"
  reliability (ORI 48/100) not from poor data but because ~418 km altitude is
  the most drag-sensitive regime — the same physics that forces frequent
  station reboosts.
- **Reliability varies by population**: station-group objects score worse
  (median ORI 45.0) than cubesats (61.1) and science satellites (68.4)
  purely through orbital regime.

## Repository Structure

```text
src\    ingestion, loading, quality gate, orchestration, analysis, API
sql\    SQL transformations for Gold tables (lessons 3, 4, 8, 11, 12, 23)
scripts\ Windows scheduled-task setup and removal
monitoring\ Prometheus scrape config and Grafana provisioning
.github\workflows\ continuous-integration smoke test
docker-compose.yml  MinIO + Redpanda + Prometheus + Grafana
data\   bronze/ silver/ gold/ reports/ quality/ logs/  (not committed)
```

## Design Principles

- **Bronze is immutable evidence.** Every snapshot is checksummed; the loader
  refuses to re-load or accept modified files.
- **Point-in-time correctness.** Element age is measured from each element's
  own epoch to its snapshot time, never against "now".
- **Honest science.** The report states limitations, distinguishes measured
  from republished data, and never claims collision probability.
- **Provider respect.** Per-source two-hour rate limits enforced in the
  orchestrator, independent of the hourly schedule (defense in depth).
- **Idempotency everywhere.** Every step is safe to re-run; the pipeline uses
  a lock file because DuckDB allows one writer.

## The Learning Journey

This repository was built lesson by lesson as a beginner-friendly course.
Each lesson below is self-contained: what was built, why it matters, the
commands to run it, and an exercise.

### The Simple Picture

Imagine a kitchen:

- A data source is the grocery shop.
- An ingestion script is the person bringing groceries home.
- Bronze is the unopened grocery bag.
- Silver is the cleaned and organized food.
- Gold is the finished meal: useful findings and charts.

Never change a Bronze file. It is evidence of exactly what the source sent.

## Lesson 1: First Pipeline

### 1. Open PowerShell

Open Windows Terminal or PowerShell in the repository folder (replace the
path with wherever you cloned or placed the project):

```powershell
Set-Location "D:\Projects\DataEngineer\stormtrace"
```

`Set-Location` means "move into this folder."

### 2. Run the pipeline

```powershell
python src\ingest_celestrak.py
```

The script will:

1. Ask CelesTrak for orbit records for each tracked group (currently the
   `stations`, `cubesat`, `science`, and `iridium-33-debris` groups; see
   Lessons 10, 14, and 23).
2. Validate that the response has the fields we need.
3. Save the unchanged response under `data/bronze/celestrak/`.
4. Save selected, consistently named columns under `data/silver/`.
5. Print a small summary.

### 3. Look at the files

After the command succeeds, these paths will exist:

```text
data/
|-- bronze/
|   `-- celestrak/
|       `-- stations_YYYYMMDDTHHMMSSZ.csv
`-- silver/
    `-- stations_satellites_latest.csv
```

The timestamp is UTC, the time standard used for space data.

## Words To Learn

- **API**: A doorway that lets one program request data from another program.
- **JSON**: A structured text format commonly returned by APIs.
- **CSV**: A table stored as text, similar to a simple spreadsheet.
- **Pipeline**: Ordered steps that move and transform data.
- **Ingestion**: Bringing data from its source into your system.
- **Schema**: The expected names and types of fields in data.
- **Validation**: Checking that data is usable before trusting it.
- **UTC**: A shared global clock that avoids timezone confusion.

## Learning Roadmap

Do not install all the tools at once. Each phase produces something usable.
Phases 1-7 below are complete; later phases build on them.

| Phase | What you build | What you learn | Tools | Status |
|---|---|---|---|---|
| 1 | Download and store orbital data | Python, APIs, CSV | Python | Done |
| 2 | Add NOAA space-weather data | Multiple sources, timestamps, joins | Python, JSON | Done |
| 3 | Build first Gold table with SQL | Medallion design, joins | DuckDB | Done |
| 4 | Turn orbit data into features | Orbital mechanics basics, SQL math | DuckDB | Done |
| 5 | Keep history | Idempotency, immutability, lineage | DuckDB | Done |
| 6 | One safe pipeline command | Orchestration, logging, locks | Python | Done |
| 7 | Data-quality gate | Validation, severity levels | DuckDB | Done |
| 8 | First research analysis | Orbit change, charts, honest reporting | matplotlib | Done |
| 9 | Automatic snapshot collection | Task Scheduler, rate-limit defense | Windows | Done |
| 10 | Expand tracked population | Multi-group ingestion, migrations | DuckDB | Done |
| 11 | Element freshness analysis | Point-in-time age, percentiles | DuckDB | Done |
| 12 | Orbit Reliability Index | Explainable scoring, documentation | DuckDB | Done |
| 13 | GitHub packaging | Version control, reproducibility | Git | Done |
| 14 | Science group and continuous integration | Population growth, CI discipline | GitHub Actions | Done |
| 15 | SGP4 propagation disagreement | Orbit propagation, RIC frames, migration | sgp4 | Done |
| 16 | First real measurements, partial-load incident | Transactions, atomicity, incident response | DuckDB | Done |
| 17 | Validate ORI against measurements | Point-in-time backtesting, Spearman correlation | Python | Done |
| 18 | Status API | Serving research outputs, graceful degradation | FastAPI | Done |
| 19 | MinIO lakehouse | Object storage, S3 API, SQL-over-S3 | Docker, MinIO, httpfs | Done |
| 20 | Streaming events | Kafka API, topics, keys, consumer groups | Redpanda, confluent-kafka | Done |
| 21 | Learning the index | GroupKFold, leakage control, MLflow tracking | scikit-learn, MLflow | Done |
| 22 | Dashboards and monitoring | Prometheus semantics, dashboards as code | Grafana, Prometheus | Done |
| 23 | The three-pillar dataset | Debris population, daily indices, telemetry metadata, versioned APIs | CelesTrak SW, SatNOGS | Done |
| 24 | Monitoring the research pillars | Pillar gauges on /metrics, dashboard growth, regression verification | Prometheus, Grafana | Done |

## Rules For Scientific Honesty

- Public orbit elements are estimates, not exact satellite locations.
- A later orbit element is not perfect ground truth.
- StormTrace does not replace an official collision-warning service.
- Call the measured difference "public orbit propagation disagreement."
- Record source time, retrieval time, and model version for every result.

## Data Source

Lesson 1 uses CelesTrak's public GP endpoint:

<https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=csv>

Use the service politely. CelesTrak's current policy says GP data updates every
two hours. If you receive HTTP 403, stop and wait rather than repeatedly
retrying. We will add the larger `active` group to a scheduled pipeline later.

## Lesson 2: Space Weather

Satellites move through Earth's extremely thin upper atmosphere. Space weather
can heat and expand that atmosphere, increasing drag on low satellites. We
need observations from upstream of Earth to study this relationship.

Run:

```powershell
python src\ingest_noaa.py
```

The script downloads two NOAA feeds covering approximately the latest 24
hours:

- Magnetic field data, including `Bz` in GSM coordinates
- Solar-wind plasma data, including proton speed, temperature, and density

It creates:

```text
data/
|-- bronze/
|   `-- noaa/
|       |-- magnetic_field_YYYYMMDDTHHMMSSZ.json
|       `-- plasma_YYYYMMDDTHHMMSSZ.json
`-- silver/
    |-- noaa_magnetic_field_latest.csv
    `-- noaa_plasma_latest.csv
```

NOAA may include observations from several spacecraft. The
`is_active_source` column tells us which source NOAA currently considers
operational for that measurement. Missing measurements are normal and must not
be changed to zero.

### Lesson 2 Exercise

Open the two Silver CSV files and find:

1. The newest `observed_at_utc` value.
2. The names in the `spacecraft` column.
3. Rows where `is_active_source` is `True`.
4. Rows with an empty measurement.

Notice that magnetic and plasma timestamps can differ by several seconds. In
Lesson 3, SQL will safely match observations by time instead of assuming row 1
in one file belongs to row 1 in the other.

NOAA source directory:

<https://services.swpc.noaa.gov/json/rtsw/>

## Lesson 3: First Gold Table With SQL

Now we turn two cleaned tables into one useful table. SQL is a language for
asking questions about tables. DuckDB is a small free database that runs on
your computer and is excellent for data analysis.

Install DuckDB once:

```powershell
python -m pip install duckdb
```

Then build the Gold table:

```powershell
python src\build_gold.py
```

The script reads the two Lesson 2 Silver files and creates:

```text
data\stormtrace.duckdb
data\gold\space_weather_minute.csv
```

The two NOAA feeds do not always record measurements at the exact same
second. The SQL uses the UTC minute as a safe first matching key. It keeps both
original observation timestamps so we can inspect how close the measurements
really were. This is an analysis convenience, not a claim that the two
measurements happened at exactly the same instant.

### Lesson 3 Exercise

Open `sql\lesson3_gold.sql` and find these ideas:

- `WITH`: creates temporary named steps.
- `CAST`: changes text into a timestamp or number.
- `TRY_CAST`: returns an empty value instead of crashing on a bad measurement.
- `date_trunc('minute', ...)`: rounds a timestamp down to its minute.
- `LEFT JOIN`: keeps magnetic records even when no plasma row matches.
- `row_number()`: ranks multiple spacecraft records within the same minute.
- `COUNT(*)`: counts rows.

You can ask DuckDB questions by running this command:

```powershell
python -c "import duckdb; c=duckdb.connect('data/stormtrace.duckdb'); print(c.sql('SELECT magnetic_spacecraft, COUNT(*) AS rows FROM gold_space_weather_minute GROUP BY 1 ORDER BY rows DESC').fetchall()); c.close()"
```

The result shows how many Gold rows came from each magnetic-field spacecraft.
The database is local and free. We will use it before introducing larger tools
such as Spark and Iceberg.

## Lesson 4: Turn Orbit Data Into Features

The source gives us **mean motion**, which means how many times an object goes
around Earth per day. We can derive quantities that are easier to understand:

- Orbital period in minutes
- Semi-major axis, the orbit's average geometric radius
- Mean altitude above Earth
- Perigee, the lowest part of the orbit
- Apogee, the highest part of the orbit
- Age of the orbital element when we downloaded it

Run:

```powershell
python src\build_orbit_features.py
```

The command creates:

```text
data\gold\satellite_orbit_features.csv
data\gold\orbit_band_summary.csv
```

The main formula is in `sql\lesson4_orbit_features.sql`. In simple words:

```text
more orbits per day -> shorter period -> usually lower altitude
fewer orbits per day -> longer period -> usually higher altitude
```

These values are derived from public mean orbital elements. They are useful
features, not precise real-time spacecraft locations.

### Lesson 4 Exercise

Open `data\gold\satellite_orbit_features.csv` and find:

1. The object with the lowest mean altitude.
2. The object with the highest eccentricity.
3. The shortest orbital period.
4. Any row where `is_stale_over_24_hours` is `True`.
5. The difference between perigee and apogee for one object.

A large perigee-to-apogee difference means the orbit is less circular. Data
freshness is kept as its own feature because old orbital elements can create
larger propagation disagreement.

## Lesson 5: Keep History

One download is only one photograph. To find new space findings, we need many
photographs taken at different times. Bronze files are immutable snapshots;
the history loader puts them into DuckDB tables without loading the same file
twice.

Run:

```powershell
python src\load_history.py
```

You should see the existing station and NOAA Bronze files being loaded. Then
run the exact same command again. The second run should say:

```text
New rows loaded: 0
```

That behavior is called **idempotency**. It means repeating a safe data job
does not create duplicate data.

Create snapshot summaries:

```powershell
python src\summarize_history.py
```

This creates:

```text
data\gold\orbit_snapshot_summary.csv
data\gold\space_weather_snapshot_summary.csv
```

The current history has only a small number of snapshots, so it cannot prove
a scientific relationship yet. The correct routine is to collect one snapshot
every two hours or less frequently, obeying each provider's policy, for several
weeks. Never run a tight loop. Each snapshot records both the source's data
time and our download time.

### Lesson 5 Exercise

1. Run `python src\load_history.py` twice.
2. Confirm the second run loads zero new rows.
3. Open `data\gold\orbit_snapshot_summary.csv`.
4. Compare `average_bstar` between snapshots.
5. Explain why two snapshots are not enough to call something a storm effect.

The answer should mention seasonality, maneuvers, stale records, model error,
and the need for repeated quiet and storm control periods.

## Lesson 6: One Safe Pipeline Command

An **orchestrator** runs data jobs in the correct order. Before using Airflow,
learn the behavior with a small Python orchestrator.

First, ask what would happen without changing anything:

```powershell
python src\run_pipeline.py --dry-run
```

This is called a dry run. It makes no network requests and changes no data.
It reports whether each collector is due.

Run one real collection cycle:

```powershell
python src\run_pipeline.py
```

The orchestrator:

1. Checks the newest Bronze snapshot.
2. Skips network ingestion when less than two hours have passed.
3. Runs due collectors sequentially.
4. Stops if ingestion fails instead of using partial fresh data silently.
5. Loads immutable snapshots into history.
6. Rebuilds historical and current Gold tables.
7. Writes one JSON log event for every step.

Logs are stored at:

```text
data\logs\pipeline_runs.jsonl
```

JSON Lines means every line is one complete JSON object. This format is easy
for log systems, Spark, and DuckDB to read later.

### Lesson 6 Exercise

1. Run the dry run twice and confirm it never downloads data.
2. Run the real pipeline once.
3. Immediately run it again and confirm both collectors are skipped.
4. Open `data\logs\pipeline_runs.jsonl` and find `status`, `duration_seconds`,
   `stdout`, and `stderr`.
5. Find the final event where `step` is `pipeline` and `status` is `success`.

Do not create a scheduled task while first learning. Collect manually for a
day and inspect every run. Lesson 9 below shows how to automate safely once
you trust the pipeline and its quality gate.

## Lesson 7: Data-Quality Gate

A pipeline succeeding technically does not mean its data is trustworthy. A
**quality gate** tests the data before Gold tables are approved.

Run the checks directly:

```powershell
python src\check_quality.py
```

The gate checks row existence, duplicate identifiers, physical orbital ranges,
Bronze checksums, missing NOAA measurements, and active NOAA source coverage.

There are two severity levels:

- **Error**: impossible or structurally unsafe data. It returns a failure code
  and stops the pipeline before Gold is rebuilt.
- **Warning**: suspicious or incomplete sensor coverage. It appears in the
  report but does not automatically discard useful data.

Reports are written to:

```text
data\quality\latest_report.json
data\quality\latest_report.csv
```

The normal pipeline now runs this gate after loading history and before
creating Gold outputs:

```powershell
python src\run_pipeline.py
```

### Lesson 7 Exercise

1. Run `python src\check_quality.py`.
2. Open the CSV report and count pass, warn, and fail rows.
3. Find each threshold and explain why it exists.
4. Do not edit Bronze data to create a failure; Bronze is immutable evidence.
5. Explain why a missing sensor value stays empty. Zero is a measurement;
   empty means unknown.

## Lesson 8: First Research Analysis

This is the scientific heart of StormTrace. Two analyses run on your real
data:

1. **Orbit-change detection** compares each object's mean motion between
   consecutive snapshots. Mean motion increases when an orbit decays, so the
   change is a drag proxy converted into kilometers of altitude per day.
2. **Space-weather characterization** summarizes the last 24 hours into
   hourly conditions using two simple research guides:
   hourly average Bz below -5 nT, or hourly average proton speed above
   500 km/s.

Run:

```powershell
python src\analyze_research.py
```

Outputs are written to `data\reports\`:

```text
space_weather_timeline.png    Bz and proton speed over 24 hours
orbit_altitude_distribution.png  Mean altitude of every station object
orbit_decay_rates.png         Measured decay per object (needs 2+ snapshots)
research_summary.md           Honest written summary of current findings
```

The report states plainly when there is not enough data for a conclusion.
With one orbital snapshot, the change-detection machinery is built and
tested but reports `INSUFFICIENT SNAPSHOTS`. After you collect a second
snapshot two or more hours later, the same command will produce real
per-object decay rates automatically.

The pipeline now ends with this analysis:

```powershell
python src\run_pipeline.py
```

### Lesson 8 Exercise

1. Run `python src\analyze_research.py`.
2. Open `space_weather_timeline.png` and find the most southward Bz dip.
3. Open `orbit_altitude_distribution.png` and find the lowest object.
4. Read `research_summary.md` and list every honest limitation mentioned.
5. Wait at least two hours, run `python src\run_pipeline.py`, then run the
   analysis again and compare the orbit-change results.

### Why Mean Motion Is a Drag Proxy

Drag removes energy, the orbit shrinks, and a lower orbit is faster. So:

```text
more drag -> smaller orbit -> more revolutions per day
```

A positive `mean_motion_delta` between snapshots means the object's public
orbit estimate moved downward. A maneuver can also cause sudden change, which
is why the report never claims pure atmospheric drag without repeated
evidence.

## Lesson 9: Automatic Snapshot Collection

Until now, every snapshot required you to type a command. **Automation** lets
the computer collect snapshots by itself while you sleep or study.

Windows includes a tool called **Task Scheduler**. It can run a program at
set times, like an alarm clock for your pipeline.

### Enable Automation

```powershell
powershell -ExecutionPolicy Bypass -File scripts\setup_scheduler.ps1
```

This registers a task named `StormTracePipeline` that:

- Runs every hour
- Starts again as soon as possible if the computer was off or asleep
- Never starts a second copy while one is still running
- Stops any run that somehow takes longer than one hour

### Why Hourly Is Safe

The trigger is hourly, but the pipeline itself refuses to download from
CelesTrak or NOAA more often than every two hours. So the schedule can never
violate the providers' usage policies. This is **defense in depth**: two
independent protections instead of one.

The pipeline also uses a **lock file**. If a run is already writing to
DuckDB, a second run exits politely instead of corrupting data. We saw this
danger ourselves when two processes collided on the database file.

### What The Scheduled Run Does

Each hourly run:

1. Downloads only if the last snapshot is at least two hours old
2. Loads any new Bronze snapshots into history
3. Runs the data-quality gate
4. Rebuilds all Gold tables and research charts
5. Appends every step to `data\logs\pipeline_runs.jsonl`

The newest run's console output is saved to:

```text
data\logs\scheduler_last_run.log
```

### Useful Commands

```powershell
# See detailed task status
schtasks /Query /TN StormTracePipeline /V /FO LIST

# Force a run immediately without waiting for the next hour
schtasks /Run /TN StormTracePipeline

# Disable automation
powershell -ExecutionPolicy Bypass -File scripts\remove_scheduler.ps1
```

### Lesson 9 Exercise

1. Register the task with the setup script.
2. Run `schtasks /Query /TN StormTracePipeline` and find "Next Run Time".
3. Force a run with `schtasks /Run` and then read
   `data\logs\scheduler_last_run.log`.
4. Confirm `data\logs\pipeline_runs.jsonl` gained a new run.
5. Try running `python src\run_pipeline.py` while the scheduled task is
   running, and observe the lock message.
6. Leave the computer on overnight and check how many snapshots appeared.

### Important Notes

- The computer must be on (or awake) for snapshots to be taken. A sleeping
  laptop collects nothing, but missed runs are caught up when it wakes.
- Your project collects every two hours because that matches the catalog's
  own update rhythm, not because more requests would give better data.
- If CelesTrak ever returns HTTP 403, the pipeline stops cleanly and the
  next hourly attempt waits out the rate limit automatically.

## Lesson 10: Expand The Tracked Population

Twenty-two station objects cannot reveal population-level drag waves. The
central research question — do groups of satellites show correlated orbit
changes after space-weather disturbances — needs a real population. StormTrace
now also tracks CelesTrak's `cubesat` group, roughly two thousand objects,
many of them in drag-sensitive low orbits.

The tracked groups are defined in one place, `DEFAULT_GROUPS` at the top of
`src\ingest_celestrak.py`, and the pipeline imports that same list, so there
is a single source of truth. To add another group later, such as `science`,
add it to that list.

### Per-Group Policy Compliance

Each group has its own two-hour freshness rule. The orchestrator checks every
group separately and downloads only the groups that are due:

```text
celestrak/stations: not due (stations_...csv is recent; wait about ...)
celestrak/cubesat:  due (no previous snapshot)
```

A brand-new group is downloaded immediately even while other groups are still
fresh, and a fresh group is never re-downloaded just because another group
was due.

### Cross-Group Deduplication

Groups can overlap: an object may appear in more than one group. Two groups
downloaded in the same run share one snapshot timestamp, so without care the
uniqueness quality check would fail. The history loader keeps only the first
copy of each object per snapshot, preferring the curated stations group.

### Schema Migration

Existing databases were created before the `source_group` column existed.
The loader now checks the table structure and adds the column when missing,
backfilling `'stations'` for existing rows. This is a small, real example of
the migrations every production data system needs.

### Faster Bulk Inserts

The old loader inserted one row at a time, which took almost a minute for a
single NOAA snapshot. Inserts are now batched into multi-row statements,
hundreds of rows at a time, keeping the loader fast as the population grows.

### Reading Many Files In One Query

The orbit-feature SQL now reads every group's Silver file at once:

```sql
FROM read_csv_auto('data/silver/*_satellites_latest.csv', filename = true)
```

The `filename = true` option adds the source path as a column, and a regular
expression extracts the group name from it. One query, many files, one
population.

### Charts For A Population

With thousands of objects, one bar per satellite is unreadable. The altitude
chart automatically switches to a histogram when the population exceeds 30
objects, and the decay chart shows only the 25 largest measured changes.

### A Real Bug We Hit

While adding the cubesat group, the loader suddenly reported zero new rows
even though a fresh 86-record Bronze file had just been written. The cause
was a one-letter typo: the glob pattern read `celetrak` instead of
`celestrak`. Nothing had been deleted — the pattern simply matched nothing.

The lesson: **a glob that matches nothing fails silently**. It returns an
empty list, not an error. When a loader suddenly finds fewer files than
expected, print the count of discovered files and compare it against what
you know exists. Our own "Snapshots checked" line is what made the mismatch
visible.

### Lesson 10 Exercise

1. Run `python src\run_pipeline.py --dry-run` and find the per-group lines.
2. Run the pipeline and confirm which groups were downloaded or skipped.
3. Open `data\reports\orbit_altitude_distribution.png` and identify the main
   altitude clusters in the cubesat population.
4. Open `data\reports\research_summary.md` and find the population coverage
   lines and the count of objects awaiting a second snapshot.
5. Explain why cubesats need a second snapshot before any decay rate can be
   computed for them.

## Lesson 11: Element Freshness Analysis

Lesson 10 revealed that many tracked objects carry orbital elements older
than 24 hours. This lesson measures that systematically.

**Element age** is the time between an element's own epoch and the snapshot
time at which we captured it. This is the point-in-time correct measure: it
tells us exactly how fresh the public catalog was for each object at the
moment we observed it.

The analysis runs automatically with the research step:

```powershell
python src\analyze_research.py
```

It creates three new Gold tables, exported as CSV files:

```text
data\gold\element_freshness.csv    Per-object age, group, altitude band
data\gold\freshness_by_group.csv   Median and P90 age per group
data\gold\freshness_by_band.csv    Median and P90 age per altitude band
```

Plus a new chart:

```text
data\reports\element_freshness.png
```

The chart's left panel shows the element-age distribution for each group,
and the right panel shows median element age by altitude band. Both include
a 24-hour staleness guide line.

### Why Freshness Matters

Orbit propagation error grows with element age. An object whose last public
element is a day old can be kilometers away from where a naive propagation
places it. This has two consequences for the research:

1. Freshness must be reported alongside every orbit-change measurement,
   because a measured change between a fresh element and a stale one mixes
   real orbital evolution with catalog update timing.
2. Element age is a core input for the planned **Orbit Reliability Index**:
   before trusting any public orbit, ask how old it is.

The per-group and per-band summaries also quantify a real catalog behavior:
different object classes are refreshed at different rates, so reliability
varies by population.

### Lesson 11 Exercise

1. Run `python src\analyze_research.py`.
2. Open `data\reports\element_freshness.png` and compare the two groups'
   age distributions.
3. Open `data\gold\freshness_by_band.csv` and find which altitude band has
   the oldest median element age.
4. Read the freshness tables in `data\reports\research_summary.md`.
5. Explain why a decay-rate measurement involving a stale element should be
   treated more carefully than one between two fresh elements.

## Lesson 12: Orbit Reliability Index

This is the flagship product concept: one explainable number per object that
says how much its public orbit estimate should be trusted **right now**.

The index combines the three signals built in earlier lessons:

```text
freshness_score = 100 * clamp(1 - element_age_hours / 48)
drag_safety     = 100 * clamp((altitude_km - 300) / 500)
base_score      = 0.55 * freshness_score + 0.45 * drag_safety
ORI             = base_score * environment_factor
```

The **environment factor** summarizes the last three hours of space weather:
1.0 in quiet conditions, 0.9 during fast solar wind, and 0.8 with sustained
southward Bz. A storm lowers every object's index, and low-altitude objects
lose the most because their base scores already lean on drag sensitivity.

Scores map to four classes:

| Class | ORI range | Meaning |
|---|---|---|
| high | 80-100 | Public orbit is fresh and drag-insensitive |
| moderate | 60-79 | Usable with normal caution |
| reduced | 40-59 | Stale element or drag-sensitive orbit |
| low | 0-39 | Treat the public orbit as a rough estimate |

The analysis runs automatically with the research step:

```powershell
python src\analyze_research.py
```

New outputs:

```text
data\gold\orbit_reliability_index.csv      Per-object scores and components
data\gold\reliability_class_summary.csv    Class distribution
data\gold\reliability_group_summary.csv    Per-group averages
data\reports\orbit_reliability_index.png   Distribution and age scatter
```

### Why Every Weight Is Documented

The 48-hour freshness scale, the 300-800 km altitude band, the 0.55/0.45
weights, and the environment factors are **prototype choices**, stated
openly in SQL comments and in the report. They are not calibrated constants.
Once enough snapshots exist, the index will be validated against measured
propagation disagreement between consecutive element sets, and the weights
will be tuned against that evidence.

### What ORI Is Not

- It is not collision probability.
- It is not a measurement of true position error.
- It does not replace official conjunction warnings.

It is a transparent trust signal for public orbit data — the question every
user of public orbital data should ask before relying on it.

### Lesson 12 Exercise

1. Run `python src\analyze_research.py`.
2. Open `data\gold\orbit_reliability_index.csv` and pick one object.
3. Recompute its ORI by hand from the component columns.
4. Open `orbit_reliability_index.png` and explain the shape of the
   reliability-versus-age scatter.
5. Find which object currently has the lowest ORI and explain which
   component dominates its score.
6. Explain why the environment factor lowers every object's index during a
   storm even though space weather did not change any element's epoch.

## Lesson 13: GitHub Packaging

A project that lives only on one laptop is not a portfolio. Version control
makes the work reproducible, reviewable, and shareable.

### What Was Added

- `requirements.txt` with pinned dependency versions, so anyone can
  recreate the exact environment:

```powershell
pip install -r requirements.txt
```

- A professional README front matter: research question, architecture
  diagram, quick start, current findings, and design principles — the first
  thing a recruiter or reviewer reads.
- A complete `.gitignore` so downloaded data, logs, and the database are
  never committed. Data is reproducible from public sources; the code is
  the artifact.

### Why Data Is Not Committed

The `data\` directory is ignored on purpose:

1. **It is reproducible.** Anybody can re-download from the same public
   endpoints using this code.
2. **It is not ours to redistribute.** CelesTrak and NOAA provide data
   under their own usage policies; redistribution terms are theirs to set.
3. **It grows forever.** Snapshots accumulate every two hours; a git
   repository is the wrong storage engine for time-series data.

The immutable Bronze design means the *code* carries everything needed to
rebuild all Silver and Gold tables from a fresh collection.

### Publishing To GitHub

```powershell
git init
git add .
git commit -m "StormTrace: space-weather orbit reliability lakehouse, lessons 1-13"
```

Then create an empty repository on GitHub and follow its instructions, or:

```powershell
git remote add origin https://github.com/<your-username>/stormtrace.git
git push -u origin main
```

### Lesson 13 Exercise

1. Run `git status` and confirm no `data\` files appear as untracked.
2. Run `git log --oneline` after the first commit.
3. Clone your own repository into a different folder and run the quick
   start; confirm the pipeline rebuilds everything from scratch.
4. Explain why pinning dependency versions matters six months from now.

## Lesson 14: Science Group And Continuous Integration

Two additions that make StormTrace both scientifically broader and
employer-ready.

### The Science Group

Adding a tracked group is a one-line change to `DEFAULT_GROUPS` in
`src\ingest_celestrak.py`. The science group contributed 48 research
satellites and immediately produced a fresh finding:

| Group | Median element age | Stale >24h | Median ORI |
|---|---:|---:|---:|
| science | 13.9 h | 14.6% | 64.3 |
| cubesat | 15.9 h | 25.6% | 54.4 |
| stations | 17.0 h | 31.8% | 43.6 |

Research satellites are the best-tracked population in the public catalog —
fresher than cubesats and even than station-group objects — and they score
the highest reliability. The group also added 12 high-altitude objects
(averaging 64,000 km) where drag is negligible, which demonstrates the
reliability index responding to orbital regime exactly as designed.

This table is itself a research result: catalog tracking quality varies
systematically by object class, and any population-level space-weather study
must account for that bias.

### Continuous Integration

`.github/workflows/ci.yml` runs a smoke test on every push and pull request:

1. Install the pinned dependencies
2. Compile every Python source file
3. Verify every SQL file exists and is non-empty
4. Verify dependencies and the orchestrator import cleanly

The checks deliberately never contact CelesTrak or NOAA. CI runners are
shared cloud machines, and the project's provider-respect principle applies
to any machine it runs on — automated cloud requests against rate-limited
public services would be both rude and unreliable as a test.

Once pushed, every future commit shows a green check mark in GitHub — the
smallest possible signal that the repository is maintained like production
code.

### Lesson 14 Exercise

1. Add another group name to `DEFAULT_GROUPS` (browse the CelesTrak GP
   index for options), run the pipeline, and observe the new group's
   freshness statistics.
2. Read `.github/workflows/ci.yml` and explain each step in your own words.
3. Push a change and watch the Actions tab run the checks.
4. Explain why CI must not download data even though the pipeline does.

## Lesson 15: SGP4 Propagation Disagreement

This is the project's core scientific instrument. Everything so far derived
features from element *values*; this lesson measures how far an old element
set's **prediction** drifts from a newer one.

### What It Measures

For each object, when a refreshed element set appears (a later element with
a different epoch):

1. Build an SGP4 satellite from the **earlier** element.
2. Propagate it forward to the **later** element's epoch.
3. Build an SGP4 satellite from the **later** element and evaluate its
   position at its own epoch.
4. Measure the difference, decomposed into **radial**, **along-track**, and
   **cross-track** components in the reference orbit's RIC frame.

The result is the *public orbit propagation disagreement* — how far the old
public estimate drifted from where the newer public estimate places the
object. The later element is **not** perfect ground truth; this measures
disagreement between successive public estimates, which is the honest,
measurable quantity available from public data.

### Why RIC Components Matter

Raw xyz differences are hard to interpret. The RIC (Radial, In-track,
Cross-track) frame is tied to the orbit itself:

- **Radial**: altitude error
- **Along-track**: timing/phase error — where drag error accumulates
- **Cross-track**: orbital-plane error — where inclination/RAAN errors show

Drag uncertainty grows almost entirely along-track, so a storm-time signal
would appear there first.

### Validation Performed

The instrument was validated twice:

1. **Physical sanity**: ISS built from bronze CSV fields propagated to its
   own epoch gives 418.3 km altitude, 7.662 km/s speed, and a 92.9-minute
   period exactly matching the element's mean motion.
2. **Synthetic pair test**: a simulated 12-hour refresh (90 m/day decay)
   produced 28.1 km along-track, 0.03 km radial, 2.6 km cross-track — the
   textbook drag signature. The first simulation attempt forgot to advance
   the mean anomaly, and the instrument immediately flagged the resulting
   9,629 km phase teleport: it detects element inconsistencies, which is
   exactly what a research instrument must do.

### The Schema Migration

SGP4 needs the full element set (RAAN, argument of perigee, mean anomaly,
and the mean-motion derivatives), which the original history table did not
store. The loader now migrates older databases: it adds the new columns and
rebuilds the orbital history from Bronze files, which remain on disk as the
source of truth. No network access occurs during migration.

### Current Status

```text
Objects with element history: 152
Measurable element-set pairs: 0
```

All consecutive snapshots so far contain republished identical elements, so
the instrument reports nothing rather than reporting a fake zero. The moment
the catalog refreshes element epochs, real measurements appear automatically
on the next pipeline run — this is the exact quantity the Orbit Reliability
Index will be validated against.

### Lesson 15 Exercise

1. Run `python src\build_propagation_disagreement.py` and read the output.
2. Open `src\build_propagation_disagreement.py` and find where degrees
   become radians, and where rev/day becomes rad/min.
3. Explain why along-track disagreement grows faster than radial
   disagreement under drag.
4. Explain why the later element is not treated as ground truth.
5. Watch for the first measurable pair after the catalog refreshes, then
   compare its span hours with its total km.

## Lesson 16: First Real Measurements And The Partial-Load Incident

The laptop slept for twelve hours. When it woke, the scheduler's catch-up
run downloaded three fresh CelesTrak groups plus NOAA data — and then
`load_history` was killed by the orchestrator's 180-second timeout, mid-file.
What followed was a complete production incident, a self-healing system, a
repair that made things worse before making them better, and finally the
project's first real scientific measurements.

### The Incident Timeline

1. **06:41 UTC** — the catch-up run downloads everything, then
   `load_history` starts loading. Each 500-row chunk INSERT is its own
   auto-committed transaction. The timeout kills the process after one
   plasma chunk (500 rows) is committed but before the file is registered:
   **500 orphan rows** — present in history, absent from the registry.
2. **07:03 UTC** — the next hourly run finds the plasma file unregistered
   and loads it completely and correctly, registering it. The system
   **self-healed the load**, but the 500 orphans remain (the first 500
   records of the file now exist twice).
3. **The repair** — a diagnostic script counted 500 orphans; a cleanup
   script then deleted rows by source file... but between diagnosis and
   repair, step 2 had happened, so the delete removed 3,321 rows: the 500
   orphans **plus the 2,821 good, registered rows**. The registry now
   claimed rows that no longer existed.

The deeper lesson: **a repair must re-verify state immediately before it
acts, or delete surgically** (only rows not covered by the registry), never
by broad match. Diagnosis and repair were two operations with the world
changing in between.

### The Fixes

1. **Per-file transactions.** A file's rows and its registry entry now
   commit atomically. A process killed mid-file leaves an uncommitted
   transaction that DuckDB discards on reopen — orphan rows are now
   structurally impossible.
2. **Two new quality checks.** `orphan_history_rows` (history rows with no
   registry entry) and `registered_files_missing_rows` (registry entries
   with no history rows) guard the invariant in both directions. Either
   failure mode from this incident is now a red, blocking error.
3. **Realistic timeouts.** A machine that just woke from sleep runs cold:
   caches empty, antivirus scanning, twelve hours of files to checksum.
   Step timeouts rose from 180s to 300-600s.
4. **Retrograde epoch pairs.** The catalog sometimes republishes an element
   whose epoch is *older* than the previously seen element. There is no
   forward prediction to evaluate, so such pairs are counted and reported
   (`Retrograde epoch pairs: 1`) instead of being silently measured or
   silently dropped.
5. **Pairs are not objects.** An object with three snapshots yields two
   pairs, and a report once showed "awaiting a second snapshot: -26".
   Counts now distinguish objects from element-set pairs.

### The First Real Measurements

With 133 objects holding refreshed element sets, both instruments produced
real data:

```text
SGP4 propagation disagreement: 132 measured pairs
  median total 0.556 km, max 156.43 km, median span 16.26 h
Orbit decay: SNAP-3 EDDIE 3.14 km/day, DUCHIFAT-1 2.13 km/day,
  SWIFT 1.88 km/day
```

The RIC decomposition immediately separated two physical causes:

- **SWIFT**: 50.3 km disagreement, almost purely along-track (radial 0.37
  km) — the textbook **drag signature**.
- **MMS 1-4**: ~150 km disagreement dominated by the radial component, and
  apparent "decay" of ~1,089 km/day — these are formation-flying spacecraft
  in highly elliptical orbits performing **maneuvers**, not decaying.

An instrument that distinguishes maneuvers from drag by component shape is
doing exactly what a research instrument should.

### Lesson 16 Exercise

1. Read `src\load_history.py` and find where the transaction begins and
   commits. Explain why the registry insert must be inside it.
2. Read the two new checks in `src\check_quality.py` and explain what each
   direction of the invariant catches.
3. Open `data\reports\propagation_disagreement.png` and find the MMS
   outliers.
4. Explain why SWIFT's disagreement shape differs from MMS's.
5. Explain why "delete rows where source_file is not in the registry" is a
   safer repair than "delete rows where source_file equals X".

## Lesson 17: Validating The Orbit Reliability Index

The index *predicted* that stale, low-altitude public orbits are the least
trustworthy. Lesson 15's instrument finally produced the outcome data, so
the predictions could be tested. Validation answers one question: **when the
index said an orbit was unreliable, was it actually unreliable?**

### Method: Point-In-Time Correctness

For each measured disagreement pair, the ORI components are reconstructed
exactly as they stood at the **earlier element's snapshot time**:

```text
age at prediction     = earlier_snapshot_at - earlier_element_epoch
altitude at prediction = derived from the earlier element's mean motion
score                  = 0.55 * freshness + 0.45 * drag_safety
```

Scoring with current data would leak the future into the prediction; this
construction cannot. Spearman rank correlation is computed with a
pure-stdlib implementation (average ranks for ties, Pearson on ranks).

Run:

```powershell
python src\validate_ori.py
```

### Results (132 pairs, quiet space weather)

| Predicted class | Pairs | Median total (km) | Median rate (km/h) |
|---|---:|---:|---:|
| moderate | 39 | 0.197 | 0.017 |
| reduced | 72 | 0.572 | 0.041 |
| low | 13 | 2.276 | 0.103 |
| high | 8 | 75.894 | 4.740 |

Correlation evidence:

```text
Spearman score vs total km:              -0.364  (predicted direction)
Spearman score vs total km, drag-like:   -0.472
Spearman altitude vs total km, drag-like: -0.568  (strongest predictor)
Spearman element age vs total km:         -0.128  (weak, confounded)
```

### What The Validation Actually Showed

1. **The direction is validated.** Median error rises monotonically across
   moderate → reduced → low (0.2 → 0.6 → 2.3 km), exactly as predicted.
2. **Altitude is the strongest single predictor** (−0.57), stronger than
   the composite score itself. The 0.55 freshness weight deserves
   recalibration as more pairs accumulate.
3. **Element age alone is weakly informative** (−0.13, wrong direction but
   tiny). Reason: fresh elements on low-altitude objects drift faster than
   stale elements on high-altitude ones. Altitude dominates in this
   population; freshness matters within altitude bands.
4. **The high class is contaminated by maneuvers.** The MMS formation
   spacecraft fly high-altitude elliptical orbits (drag-safe → "high")
   but maneuver constantly, producing ~150 km disagreements. Maneuvers are
   an unmodeled failure mode: no public-data index can predict them
   without explicit maneuver detection.
5. **The environment factor remains unvalidated** — all data so far is
   quiet weather, so the factor is constant and cannot discriminate. The
   storm-time test must wait for a disturbed period.

### The Timezone Bug Found By The Validation

The first validation run matched zero pairs. The disagreement table's
snapshot columns were declared `TIMESTAMPTZ` but were inserted with naive
UTC datetimes, which DuckDB silently reinterpreted in the machine's local
timezone (+05:30), breaking the join. A join test with an explicit +5:30
shift matched exactly 132 rows — the diagnosis. The fix stores those
columns as plain `TIMESTAMP` (naive UTC), matching how they are inserted.
Lesson: **a silent timezone reinterpretation is invisible until two tables
must agree.**

### Lesson 17 Exercise

1. Run `python src\validate_ori.py` and read the class table.
2. Open `data\reports\ori_validation.png` and find the MMS points.
3. Explain why scoring with current data instead of point-in-time data
   would invalidate the test.
4. Argue for or against lowering the freshness weight below 0.55, using
   the correlations as evidence.
5. Propose how a maneuver-detection feature could protect the high class
   from contamination.

## Lesson 18: The Status API

Research results locked in CSV files serve only one person. A status API
turns StormTrace's Gold layer into a live, queryable product — the
difference between "I analyzed data" and "I built a service".

### Running The API

```powershell
python -m uvicorn src.api:app --port 8000
```

Then open <http://127.0.0.1:8000/docs> for interactive documentation that
FastAPI generates automatically from the code.

### Endpoints

| Endpoint | Serves |
|---|---|
| `/` | Service description and endpoint index |
| `/health` | Database reachability and table row counts |
| `/quality` | Latest data-quality report (from the JSON artifact) |
| `/space-weather` | Last-24-hour conditions: Bz, speed, disturbance hours |
| `/population` | Snapshots, tracked objects, per-group freshness |
| `/reliability` | ORI class distribution, per-group summary, 10 least reliable |
| `/reliability/{norad_id}` | One object's full reliability breakdown |
| `/disagreement` | SGP4 propagation disagreement statistics |
| `/validation` | ORI validation correlations and class bins |

Every reliability response carries the disclaimer that the index is not
collision probability — scientific honesty is part of the API contract.

### Design Decisions

1. **Per-request read-only connections.** DuckDB allows either one writer
   or multiple readers. While the hourly pipeline writes, API endpoints
   return `503 Service Unavailable` with a clear message instead of
   crashing or serving stale state silently.
2. **No TIMESTAMPTZ fetches.** Every timestamp is cast to naive UTC inside
   SQL before reaching Python. This avoids the `pytz` dependency issue
   that struck twice in earlier lessons — a timezone bug class the API now
   sidesteps by construction.
3. **The API adds no state.** It is a pure view over the Gold tables and
   the quality-report artifact; it can be stopped, restarted, or run
   alongside any pipeline schedule without coordination.

### Lesson 18 Exercise

1. Start the API and open `/docs`; try each endpoint from the browser.
2. Query `/reliability/25544` and verify the ISS's `drag_safety_score`
   explains her "reduced" class despite a fresh element.
3. Run the pipeline while the API is up and observe the `503` behavior
   during `load_history`.
4. Explain why the API reads the quality report from its JSON file
   instead of the database.
5. Propose one new endpoint that would help a satellite operator, and
   what Gold table it would read.

## Lesson 19: The MinIO Lakehouse

Everything so far lived in one DuckDB file plus folders on a laptop. Real
data platforms separate **storage from compute**: data lives in object
storage, and engines read it wherever they run. This lesson builds that
architecture locally with Docker and MinIO.

### Starting The Lakehouse

```powershell
docker compose up -d
```

That starts MinIO (an S3-compatible object store) and creates the
`stormtrace` bucket. The console is at <http://127.0.0.1:9001>
(login `minioadmin` / `minioadmin`) — browse the zones there after syncing.

### Syncing Data Into The Lakehouse

```powershell
python src\upload_to_minio.py
```

Every file under `data\bronze`, `silver`, `gold`, `reports`, and `quality`
is uploaded with its SHA-256 digest stored as object metadata. Re-running
skips objects whose digest already matches, so syncing is idempotent and
cheap — the same digest discipline as the Bronze registry.

The zone layout inside the bucket:

```text
s3://stormtrace/bronze/celestrak/stations_20260827T123130Z.csv
s3://stormtrace/silver/stations_satellites_latest.csv
s3://stormtrace/gold/orbit_reliability_index.csv
s3://stormtrace/reports/ori_validation.png
s3://stormtrace/quality/latest_report.json
```

### Querying The Lakehouse Directly

```powershell
python src\query_minio.py
```

DuckDB's `httpfs` extension speaks the S3 API, so `read_csv_auto` scans
objects inside MinIO as if they were local files. The demo counts all
bronze orbital rows, groups objects by source, computes the cubesat
population's median altitude from mean motion — entirely inside the
object store. Nothing is downloaded to the laptop.

This is the essential lakehouse trick: **compute travels to the storage**.

### Pipeline Integration

The pipeline's final step is now `sync_minio`. When MinIO is running, each
run uploads exactly the files it regenerated (typically five). When MinIO
is stopped, the step exits with code 2, which the orchestrator logs as
`skipped` — the pipeline still succeeds. Optional infrastructure must
never break the core pipeline.

### Why This Matters For Hiring

Object storage plus SQL-over-S3 is the core of every modern data platform
(S3 + Athena, GCS + BigQuery external tables, ADLS + Databricks). This
lesson demonstrates the pattern with the same APIs, locally, on a student
laptop: Docker orchestration, an S3-compatible store, idempotent sync,
and a query engine reading straight from the bucket.

### Lesson 19 Exercise

1. Run `docker compose up -d`, sync, then browse the bucket in the MinIO
   console at port 9001.
2. Run the sync twice and confirm the second run uploads nothing.
3. Stop MinIO (`docker compose stop minio`), run the pipeline, and confirm
   `sync_minio` is logged as skipped while everything else succeeds.
4. Modify `src\query_minio.py` to compute the stations group's median
   altitude from the lakehouse.
5. Explain why storing the SHA-256 in object metadata makes the sync
   idempotent without a local manifest.

## Lesson 20: Streaming Events With Redpanda

Batch pipelines report through log files; event-driven systems publish what
happened as it happens, and anything can subscribe. This lesson adds a
Kafka-compatible stream to StormTrace: the pipeline's step events now flow
onto a Redpanda topic, and a consumer reads them back live.

### The Pieces

```powershell
docker compose up -d          # starts MinIO + Redpanda (Kafka API, port 9092)
python src\publish_events.py  # publishes the latest completed run's events
python src\consume_events.py --from-beginning   # replays the topic
python src\consume_events.py                     # tails live events
```

- **Topic**: `stormtrace.pipeline.events` — one JSON message per pipeline
  step event, keyed by `run_id`.
- **Keying**: all events of one run share a key, so they land in the same
  partition and every consumer sees each run's steps **in order**. Kafka
  guarantees order only within a partition; key-by-run is how you get it.
- **Headers**: each message carries an `event_step` header, so consumers
  can filter by step without parsing the JSON body.

The JSONL log file remains the source of truth; the stream is a projection
of it. The publisher emits the most recently **completed** run (including
its final `pipeline` summary event), which keeps the stream's runs whole.

### The Advertised-Listeners Lesson

The first connection failed with `Failed to resolve 'redpanda:9092'` —
from the laptop. Kafka brokers advertise the address clients should
**reconnect to**, and the broker was advertising its Docker-internal
hostname. The fix is dual listeners:

```text
internal://redpanda:9092   for containers
external://localhost:9092  for the host Python client
```

This exact misconfiguration is one of the most common real-world Kafka
deployment bugs; meeting it here, on a laptop, is the cheapest way to learn
it.

### Optional Infrastructure, Again

`publish_events` is a pipeline step that exits with code 2 when the broker
is unreachable. The orchestrator logs that as `skipped`, and the run still
succeeds — the same contract as `sync_minio`. The core pipeline depends on
neither Docker service; both enrich it when present.

A second hardening was needed along the way: `flush()` returns the count of
still-undelivered messages, and only by checking that return value can a
producer distinguish "published" from "queued and silently dropped."

### Lesson 20 Exercise

1. Publish, then replay, the events; match each line to a step in the
   last pipeline run.
2. In a second terminal, run the live consumer, then run the pipeline in
   the first; watch the events arrive as the run progresses.
3. Stop Redpanda, run the pipeline, and confirm `publish_events` is
   logged as skipped.
4. Explain why events are keyed by `run_id` rather than by step name.
5. Propose a second topic (for example, quality-gate failures) and what
   would publish to it.

## Lesson 21: Learning The Index — ML Model With MLflow

The Orbit Reliability Index encodes hand-designed physics: freshness and
drag sensitivity weighted 0.55/0.45. This lesson asks whether a model can
**learn** the relationship between point-in-time features and measured
disagreement — and whether it beats the hand-crafted index.

    python src\train_model.py

### Setup

- **Features (point-in-time correct)**: exactly what was knowable at the
  earlier element's snapshot — altitude, element age, inclination,
  eccentricity, bstar, source group. Nothing from the future.
- **Target**: `log1p` of the disagreement rate (km/h), because rates span
  orders of magnitude.
- **Validation**: `GroupKFold` grouped by NORAD id. The same object can
  appear in several measured pairs; random splits would leak an object's
  behavior into the test set. Grouping makes every test object unseen.
- **Baseline**: predicting the training median in the same folds. A model
  is only useful if it beats the baseline.

### Results (209 pairs, 138 objects)

| Metric | Model | Baseline |
|---|---:|---:|
| MAE (log) | 0.077 | 0.098 |
| RMSE (log) | 0.172 | 0.356 |
| Pooled Spearman (vs true rate) | **0.565** | 0 by construction |

Reference: the hand-crafted ORI scores Spearman −0.25 against the rate
(sign flipped: higher score = lower expected error).

### The Finding: Eccentricity

Feature importance from the learned model:

```text
eccentricity_at_prediction     0.493
mean_altitude_km_at_prediction 0.358
element_age_hours_at_prediction 0.060
bstar_at_prediction            0.054
inclination_degrees            0.034
```

The model's strongest signal — **eccentricity** — is one the hand-crafted
index does not use at all. Physically it makes sense: SGP4 mean-element
error grows fastest on highly elliptical orbits (the MMS and CXO outliers
were exactly this signature). The learned model independently confirms the
validation finding that altitude dominates element age, and it discovers
that orbital shape matters more than either weight suggests.

**Actionable insight for the index**: the ORI should gain an eccentricity
term. That is the kind of evidence-driven redesign validation exists to
produce.

### MLflow Tracking

Every training run is logged to a local SQLite-backed MLflow store
(`mlflow.db`, git-ignored): parameters, per-fold and aggregate metrics,
feature importances with honest limitations, and the model artifact.

Inspect the experiment:

```powershell
python -m mlflow ui --backend-store-uri sqlite:///mlflow.db
```

Note: MLflow's classic file-based store is in maintenance mode; the
SQLite backend is the recommended local option, and the training script
was migrated to it after hitting that exact error.

### Why The Model Is Not In The Pipeline

Training runs deliberately, not hourly. An hourly retrain would churn the
model on nearly identical data, produce drifting metrics, and bury real
changes in noise. Retrading cadence is a decision, not a default — collect
more snapshots (especially storm-time data), then retrain and compare runs
in MLflow.

### Lesson 21 Exercise

1. Run the trainer and open the MLflow UI; find the feature-importance
   artifact and the limitations logged with the run.
2. Explain why GroupKFold by object is required here and what a random
   split would inflate.
3. Compare the model's pooled Spearman (0.565) with the ORI's −0.25
   against the rate; explain why the signs differ.
4. Argue for or against adding an eccentricity term to the ORI using
   both the model's importance and the MMS/CXO outlier signatures.
5. Re-run training after a few more snapshot cycles and compare the two
   runs side by side in the MLflow UI.

## Lesson 22: Dashboards With Prometheus And Grafana

The final roadmap layer: operational visibility. Research numbers locked in
CSVs and reports serve one person at one moment; a dashboard makes the
system observable continuously. This is the classic monitoring stack —
Prometheus scrapes, Grafana visualizes — fed by a new `/metrics` endpoint
on the status API.

### Running The Stack

```powershell
docker compose up -d                      # MinIO + Redpanda + Prometheus + Grafana
python -m uvicorn src.api:app --port 8000  # the scrape target
```

Then open **http://127.0.0.1:3000** — the "StormTrace Research Status"
dashboard appears without any clicks: datasource and dashboard are
provisioned from `monitoring\grafana` (dashboards as code). Anonymous
read-only access is enabled for the demo; the admin password is `admin`.

Prometheus is at http://127.0.0.1:9090 (check Targets to see the API
being scraped every 30 seconds).

### The /metrics Endpoint

The API exposes 54 Prometheus gauges computed fresh per scrape from the
Gold layer: quality-gate failures, tracked objects, disagreement
statistics, ORI class counts, the environment factor, per-group
freshness, space-weather conditions, and the three research pillars
added in Lesson 24 (daily indices, the debris cohort, SatNOGS activity).

Two Prometheus semantics shaped the design:

1. **A scrape returns 200 even when the database is busy.** REST instincts
   say 503; monitoring instincts say the metric `stormtrace_database_reachable
   0` carries the signal — because an *absent* metric is itself an
   alertable condition, the scrape must succeed for anything to be
   observable at all.
2. **Gauges are point-in-time snapshots.** Prometheus stores the history;
   the endpoint never needs to answer "over what window?".

Two bugs were found and fixed while building it, both instructive:
`Gauge` constructed with an empty label-names list rejects `.labels()`,
and constructing the same metric name twice in one registry raises
`DuplicateTimeseries` — so labeled gauges are built once per
(name, label-shape) and cached.

### The No-Data Incident (and the verification it forced)

The first deployment rendered eight panels as "No data" while others
worked. Investigation traced two distinct causes:

1. **Seven stat panels had no query targets at all.** They were authored
   with thresholds, units, and descriptions — everything except the
   `targets` array that tells Grafana what to query. A panel with display
   configuration but no query always renders "No data": Grafana cannot
   display what it never asked for. The panels that worked were exactly
   the ones that had targets.
2. **The ORI Validation panel used a pattern unlike any working panel**:
   a single regex target returning five identically-named series with a
   label-interpolated legend and a non-default text mode. The data chain
   was verified healthy end-to-end — the API emitted the metrics,
   Prometheus returned five series, and Grafana's own `/api/ds/query`
   returned frames with values — so the panel was rebuilt in the
   structure proven to render on this Grafana version: separate targets
   with explicit label matchers and static legend formats, exactly like
   the working Space Weather panel.

The process lesson is the point: verifying that a provisioned dashboard
*exists* with N panels is not verification. A new tool now guards this:

```powershell
python src\verify_dashboard.py
```

It fetches the dashboard Grafana actually serves and runs every panel's
expressions through Grafana's own query API — the same path the browser
panels use — then fails if any panel has no targets or any target returns
no data. Run it after every dashboard change; it would have caught this
incident before any screenshot did.

### The Dashboard

Seventeen panels:

- **Stats**: Database, Quality Failures, Environment Factor, Objects
  Tracked, Measured Pairs, Median/Max Disagreement, ORI Validation
  Spearman, Space Weather
- **Pillar stats (Lesson 24)**: Debris Cohort, Debris Stale %, Peak Daily
  Kp Sum, Latest Kp Sum, Latest F10.7, SatNOGS Activity
- **Time series**: Reliability Class Distribution (stacked) and Median
  Element Age by Group

The design intent: during a geomagnetic storm, the Environment Factor
stat drops, space-weather hours climb, and the low/reduced reliability
classes swell — the system-wide stress signal, visible at a glance. That
is the exact event the project is still waiting to capture.

### Lesson 22 Exercise

1. Bring the stack up, open the dashboard, and confirm every panel has
   data.
2. Stop the API and watch `stormtrace_database_reachable` disappear from
   Prometheus (up != 1 in the query browser).
3. Run the pipeline while the API is up and watch a scrape land during
   the write lock (database_reachable dips to 0).
4. Add one panel of your own: for example, a time series of
   `stormtrace_disagreement_median_km`.
5. Explain why the metrics endpoint must not raise 503 when the database
   is busy.
6. Run `python src\verify_dashboard.py`, then delete one panel's targets
   from the dashboard JSON and confirm the script fails on it.

## Lesson 23: The Three-Pillar Research Dataset

Orbit elements and solar-wind plasma gave StormTrace a drag proxy and a
live disturbance flag, but the physics chain was incomplete. This lesson
closes the three gaps that turn the project from "drag proxy" into a
testable physical chain:

```text
daily solar indices (Kp, Ap, F10.7)      <- explanatory variable
   -> atmosphere heats and expands
   -> drag changes on low orbits
orbit decay (mean motion, bstar)         <- measured effect
telemetry activity (SatNOGS passes)      <- independent observability
```

### 1. A True Debris Population

`iridium-33-debris` joined `DEFAULT_GROUPS`, adding **111 objects** from a
single collision: a tight ~780 km band, no maneuvers, and the highest drag
terms in the catalog. They are the cleanest natural drag experiment
available — station-keeping cannot contaminate their signal. Adding a
group stayed a one-line change, and the pipeline now tracks **263
distinct objects** across four groups.

### 2. Daily Geomagnetic And Solar Indices

A new collector (`ingest_space_weather.py`) downloads CelesTrak's
SW-Last5Years file: the eight 3-hour planetary K indices and their daily
sum, the Ap average, sunspot number, and observed F10.7. Two thousand
days of history arrived in the first run, covering solar minimum into the
current maximum.

Two real data lessons surfaced immediately:

- **CelesTrak stores Kp in tenths.** A file value of `67` means Kp 6.7.
  Bronze and Silver keep the file's own values as evidence; a Gold-table
  comment explains the `/10.0` normalization so Kp sums (max 72) sit on
  the familiar scale. **The measure** `max_kp_sum 67.0` lands exactly on
  the 2024-05-11 storm — the strongest day since 2003.
- **The last day is provisional and revised.** The history table keeps
  every revision via content-hash dedupe (new `(date, values)` combos
  append; unchanged days skip). Gold picks the newest snapshot per date.

### 3. SatNOGS Telemetry Observations

`ingest_satnogs.py` samples the SatNOGS network API: which satellite
(NORAD id) was heard, by which station, when, and at what frequency.
Honest scope: this is observation **metadata**, not decoded frames.

Two versioned-service lessons came free:

- **The API changed under us.** The first run hit HTTP 400: SatNOGS now
  rejects the `page=N` parameter it previously supported. The collector
  now samples whatever the first page returns — a rolling 25-observation
  sample — and treats a changing service surface as a soft constraint,
  never a pipeline failure. Always probe a public API before and after
  wiring it in; they do not stay still.
- **`sat_id` became a string.** The API changed this field from an
  integer to a UUID string (`HADL-0708-3181-3728-3212`). The history
  table's column widened to `VARCHAR` with an in-place migration, and
  the loader stopped coercing it. The quality gate now guards the new
  tables the same way it guards every other history table.

### New Gold Tables, Chart, And Run Output

Three new Gold tables export as CSV, and one new chart joins the report:

```text
data\gold\sw_index_daily.csv       Newest daily Kp/Ap/F10.7 per date
data\gold\satnogs_activity.csv     Daily hears, satellites, stations
data\gold\debris_population.csv    The 111-object debris cohort
data\reports\sw_indices_timeline.png  Kp sum and F10.7 over 5 years
```

The research report and console summary now print one section per pillar.
A first look shows the dataset is immediately useful: the debris cohort
carries the highest drag exposure (45.9% stale elements, median ORI 53.1),
and its large disagreements already dominate the measured-pairs list —
these are the objects whose decay should respond first when the daily
indices rise.

### Per-Source Rate Limits, Precisely

`run_pipeline.py` now accepts a per-source interval: orbital groups and
NOAA stay at two hours, SatNOGS at two hours, and the space-weather file
at CelesTrak's own three-hour update cycle. One signature, one dry-run
line per source:

```text
celestrak/iridium-33-debris: due (no previous snapshot)
ingest_space_weather: not due (data\bronze\spaceweather\sw_....csv is recent; wait about 172 more minutes)
ingest_satnogs: not due (data\bronze\satnogs\observations_....json is recent; wait about 116 more minutes)
```

### Lesson 23 Exercise

1. Run `python src\run_pipeline.py --dry-run` and find the three new
   per-source lines and their different intervals.
2. Open `data\gold\sw_index_daily.csv` and find the strongest day in the
   last five years by `kp_sum`; confirm it matches the 2024-05-11 storm.
3. Open `data\reports\sw_indices_timeline.png` and relate F10.7's rise to
   the solar cycle phase.
4. Open `data\gold\debris_population.csv` and confirm every object shares
   one tight altitude band.
5. Explain why the satellite id column is `VARCHAR` and why the SatNOGS
   collector fetches one page instead of two.

## Lesson 24: Monitoring The Research Pillars

Lesson 22 proved the monitoring chain (endpoint -> Prometheus -> Grafana)
and Lesson 23 added the three-pillar dataset. This lesson closes the loop:
`/metrics` and the dashboard now expose the pillars themselves, so the
system status screen tells the same story as the research report.

Seventeen gauges joined `/metrics`, grouped by pillar:

```text
Daily indices (gold_sw_index_daily)
  stormtrace_sw_index_days          2110 days, newest revision per date
  stormtrace_sw_max_kp_sum_5y       67.0  <- 2024-05-11 storm
  stormtrace_sw_max_ap_5y / _max_f10_7_5y
  stormtrace_sw_latest_kp_sum / _latest_ap_avg / _latest_f10_7

Debris cohort (gold_debris_population)
  stormtrace_debris_objects         111
  stormtrace_debris_median_altitude_km / _median_bstar
  stormtrace_debris_stale_percent

SatNOGS activity (gold_satnogs_activity + history)
  stormtrace_satnogs_days / _observations / _good_observations
  stormtrace_satnogs_usable_observations
  stormtrace_satnogs_distinct_satellites / _distinct_stations
```

Six stat panels joined the "StormTrace Research Status" dashboard,
each written in the structure proven by the Lesson 22 incident — a single
target with a `refId` and no label-interpolated legend. The regression
tool was the arbiter again:

```powershell
python src\verify_dashboard.py
# All 17 panels verified: every target returns data.
```

The honest verification moment matters: the new metric names existed only
seconds after the API restarted, Prometheus had already scraped them, and
Grafana's own `/api/ds/query` returned frames for every new panel — the
same check that caught the eight "No data" panels in Lesson 22.

### Lesson 24 Exercise

1. Restart the API and confirm the 17 pillar gauges appear on `/metrics`.
2. Re-run `python src\verify_dashboard.py`; every panel must return data,
   not just the original eleven.
3. Compare `Peak Daily Kp Sum` (67) with `Latest Kp Sum` on the dashboard:
   the gap is exactly the storm data the project is still waiting to
   capture live.
4. The gauge cache builds one collector per (name, label-shape) — explain
   why that stays necessary now that labeled pillar gauges exist.

With Lesson 22, the planned roadmap is finished. StormTrace now spans the
full modern data-platform arc on a student laptop: automated ingestion
with provider-respecting rate limits, an immutable Bronze lakehouse with
checksummed lineage, a quality gate that blocks bad data, SGP4 science
instruments, a validated reliability index, a learned model that found
what the index missed, an API, an event stream, object storage with
SQL-over-S3, and monitoring — each layer added only when it solved a
problem the previous layer exposed.

What remains is growth: more snapshots, and above all the storm-time
data that the environment factor has never seen.
