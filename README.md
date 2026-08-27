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
CelesTrak GP (stations, cubesat)      NOAA SWPC RTSW
        |                                   |
        v                                   v
  ingest_celestrak.py                 ingest_noaa.py
        |                                   |
        +------- immutable Bronze ----------+
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
        data\gold\*.csv  +  data\reports\*.png / research_summary.md
```

Orchestration: `run_pipeline.py` (lock-protected, policy-aware) or the
registered Windows scheduled task `StormTracePipeline` (hourly, with
two-hour per-source rate limiting).

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
- **Element freshness varies by object class**: median public element age is
  ~16 hours; roughly a quarter of tracked objects carry elements older than
  24 hours, and one object (CROCUBE) carried a 66-hour-old element.
- **Low altitude dominates unreliability**: the ISS scores only "reduced"
  reliability (ORI 48/100) not from poor data but because ~418 km altitude is
  the most drag-sensitive regime — the same physics that forces frequent
  station reboosts.
- **Reliability varies by population**: station-group objects score worse
  (median ORI 43.6) than cubesats (54.4) purely through orbital regime.

## Repository Structure

```text
src\    ingestion, loading, quality gate, orchestration, analysis
sql\    SQL transformations for Gold tables (lessons 3, 4, 8, 11, 12)
scripts\ Windows scheduled-task setup and removal
.github\workflows\ continuous-integration smoke test
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
   `stations`, `cubesat`, and `science` groups; see Lessons 10 and 14).
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
| 15 | Validate ORI against measurements | Backtesting, calibration | Python | Planned |
| 16 | Move data into a local lakehouse | Object storage, table formats | Docker, MinIO, Iceberg | Planned |
| 17 | Process larger history | Distributed processing | Spark | Planned |
| 18 | Add real-time events | Streaming and event time | Kafka/Redpanda | Planned |
| 19 | Create models | Features, backtests, model tracking | scikit-learn, MLflow | Planned |
| 20 | Publish a usable product | APIs, dashboards, monitoring | FastAPI, Grafana | Planned |

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
