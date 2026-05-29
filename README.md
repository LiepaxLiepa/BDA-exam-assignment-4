# AIS Vessel Collision Detection — Big Data Examination

This repository contains a Dockerized PySpark solution for detecting the closest collision-level physical proximity between two moving vessels using Danish AIS data. The program processes large-scale temporal and spatial AIS records, filters the required study period and geographic area, removes likely noise and stationary vessels, identifies the closest valid vessel pair, and saves a 20-minute trajectory visualization centred on the detected closest-approach event.

## Assignment scope

The analysis follows the examination requirements:

- **Data source:** Danish AIS data from <http://aisdata.ais.dk/>
- **Period:** 2021-12-01 00:00:00 to 2021-12-31 23:59:59
- **Study area:** 50 nautical mile radius around latitude `55.225000`, longitude `14.245000`
- **Framework:** PySpark
- **Environment:** Docker
- **Output:** detected vessel pair, MMSI numbers, vessel names, timestamp, coordinates, closest distance, and a trajectory plot from 10 minutes before to 10 minutes after the event

## Folder structure

```text
New folder/
├── src/
│   └── collision_detection.py
├── data/
│   ├── aisdk-2021-12-01.csv
│   ├── aisdk-2021-12-02.csv
│   ├── ...
│   └── aisdk-2021-12-31.csv
├── output/
│   ├── collision_results.txt
│   └── collision_trajectory.png
├── spark-tmp/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

The raw AIS CSV files are not committed to Git because they are large. To reproduce the analysis, place the December 2021 AIS CSV files in the `data/` folder before running the container.

## Input data

The expected input files are the daily Danish AIS CSV files for December 2021:

```text
data/aisdk-2021-12-01.csv
data/aisdk-2021-12-02.csv
...
data/aisdk-2021-12-31.csv
```

The default input pattern is configured as:

```yaml
INPUT_GLOB: aisdk-2021-12-*.csv
```

This allows Spark to read all December files as one distributed dataframe.

## How to build and run

Open PowerShell in the project folder:

```powershell
cd "your folder"
```

Create the required folders if they do not exist:

```powershell
mkdir data -Force
mkdir output -Force
mkdir spark-tmp -Force
```

Place the December 2021 AIS CSV files inside `data/`.

Build and run the Docker container:

```powershell
docker compose up --build
```

To rerun from a clean state:

```powershell
docker compose down
Remove-Item .\output\* -Force
Remove-Item .\spark-tmp\* -Recurse -Force
docker compose up --build
```

After the run finishes, inspect the results:

```powershell
type output\collision_results.txt
start output\collision_trajectory.png
```

## Configuration

The application is configured through environment variables in `docker-compose.yml`.

| Variable | Example value | Meaning |
|---|---:|---|
| `DATA_DIR` | `/data` | Folder inside Docker containing the input CSV files |
| `OUTPUT_DIR` | `/output` | Folder where result files are saved |
| `INPUT_GLOB` | `aisdk-2021-12-*.csv` | Input file pattern for the December AIS files |
| `START_TS` | `2021-12-01 00:00:00` | Start of the assignment time period |
| `END_TS` | `2021-12-31 23:59:59` | End of the assignment time period |
| `CENTER_LAT` | `55.225000` | Latitude of the search-area centre |
| `CENTER_LON` | `14.245000` | Longitude of the search-area centre |
| `RADIUS_NM` | `50` | Search radius in nautical miles |
| `COLLISION_DISTANCE_M` | `500` | Maximum allowed distance for a collision candidate |
| `MIN_COLLISION_DISTANCE_M` | `0.5` | Minimum distance used to remove exact duplicate AIS overlaps |
| `TIME_TOLERANCE_SECONDS` | `120` | Maximum allowed timestamp difference between compared AIS records |
| `MIN_RECORD_SOG_KNOTS` | `0.5` | Removes stationary vessel records when SOG is available |
| `MAX_RECORD_SOG_KNOTS` | `50` | Removes records with unrealistic reported speed |
| `MAX_IMPLIED_SPEED_KNOTS` | `80` | Removes GPS jumps between consecutive records of the same MMSI |
| `EXCLUDE_RESCUE_VESSELS` | `true` | Excludes vessels with `rescue` in the AIS name |
| `SPATIAL_CELL_DEG` | `0.01` | Spatial grid cell size used for candidate generation |
| `TRAJECTORY_WINDOW_MIN` | `10` | Number of minutes before and after the event shown in the plot |

## Methodology

### 1. Data loading

The AIS CSV files are loaded with PySpark. All columns are first read as strings, and the required numeric columns are cast afterwards. This is safer for raw AIS data because missing values, unknown names, and inconsistent fields are common.

The code also normalizes column names. For example, Danish AIS files may contain a first column named `# Timestamp`; this is cleaned to `Timestamp` so that the downstream code can use stable column names.

### 2. Temporal filtering

The dataset is restricted to the exact examination period:

```text
2021-12-01 00:00:00 to 2021-12-31 23:59:59
```

Rows outside this period or rows with unparseable timestamps are removed.

### 3. Geographic filtering

The required study area is a 50-nautical-mile radius around:

```text
Latitude  = 55.225000
Longitude = 14.245000
```

The filtering is performed in two stages:

1. A fast latitude/longitude bounding box is applied first.
2. A Haversine distance calculation is then used to keep only records inside the exact 50 nm radius.

This avoids applying expensive distance calculations to the full raw dataset.

### 4. Moving-vessel and noise filtering

AIS data can contain stationary vessels, harbour traffic, GPS jumps, duplicate positions, and non-vessel transmitters. The cleaning step removes likely false collision sources using these rules:

| Noise source | Filtering rule |
|---|---|
| Missing identity or position | Drop null `MMSI`, `Latitude`, or `Longitude` |
| Invalid coordinates | Keep only valid latitude and longitude ranges |
| AIS sentinel values | Drop invalid sentinel-like positions such as latitude `91.0` |
| Stationary vessels | Drop records with `SOG < 0.5 kn` when SOG is available |
| Anchored or moored vessels | Drop statuses such as `At anchor`, `Moored`, `Aground`, and `Not under command` |
| Non-vessels | Drop `Base Station` and `AtoN` records |
| Speed outliers | Drop records with `SOG > 50 kn` |
| GPS jumps | Remove short-time movements with implied speed above `80 kn` |
| Exact duplicate overlaps | Remove candidate pairs below `MIN_COLLISION_DISTANCE_M` |
| Rescue operations | Exclude vessel names containing `rescue` in the final run |

The rescue-vessel exclusion was added after validation because the closest sustained interactions were between rescue vessels operating together in the same small area. Such tracks likely represent coordinated rescue, harbour, or training activity rather than accidental vessel collision. This is treated as an additional data-quality filter and is explicitly configurable through `EXCLUDE_RESCUE_VESSELS`.

### 5. Candidate generation and computational optimization

A full Cartesian self-join would be computationally inefficient. Instead, the code uses time and spatial partitioning:

1. Each AIS record receives a one-minute time bucket.
2. Each record receives latitude and longitude grid-cell identifiers.
3. Candidate records are compared only within neighbouring time and spatial cells.
4. A cheap coordinate-difference filter is applied before the exact Haversine calculation.
5. Only candidates within the distance threshold and time tolerance are retained.
6. The closest valid candidate is selected using Spark ordering.

This approach avoids comparing every vessel record against every other vessel record and keeps the computation feasible for the full December dataset.

## Final detected event

The final detected non-rescue close-proximity candidate was:

| Field | Value |
|---|---:|
| Vessel A MMSI | `111219512` |
| Vessel A Name | Unknown in AIS, reported as `MMSI 111219512` |
| Vessel B MMSI | `232018267` |
| Vessel B Name | `MV SCOT CARRIER` |
| Timestamp A | `2021-12-13 07:22:14` |
| Timestamp B | `2021-12-13 07:21:21` |
| Time difference | `53 s` |
| Estimated collision latitude | `55.241478` |
| Estimated collision longitude | `14.227597` |
| Closest recorded approach | `0.56 m` |
| SOG A | `1.6 kn` |
| SOG B | `0.7 kn` |

Because AIS messages are not always synchronized between vessels, the two records are separated by 53 seconds. Therefore, this event is interpreted as the closest recorded collision-level proximity detected from the AIS data.

The generated plot `output/collision_trajectory.png` shows both vessel trajectories from 10 minutes before to 10 minutes after the closest approach.

## Output files

The program writes:

```text
output/collision_results.txt
output/collision_trajectory.png
```

The text file includes:

- MMSI numbers of both vessels
- vessel names if available
- AIS timestamps of both records
- estimated collision coordinates
- closest recorded approach distance
- vessel speeds over ground

The plot visualizes the two trajectories over the required 20-minute window.

## Limitations

AIS data is observational and not perfectly synchronized between vessels. 

The algorithm removes obvious stationary records, GPS jumps, duplicate-position artefacts, and rescue-vessel interactions, but some uncertainty remains because AIS position reports can be delayed, missing, duplicated, or affected by measurement error.

## Requirements

The Python dependencies are listed in `requirements.txt`:

```text
pyspark==3.5.1
matplotlib==3.8.4
numpy==1.26.4
pandas==2.2.2
```