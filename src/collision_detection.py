from __future__ import annotations

import math
import os
import sys
import logging
from datetime import timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, TimestampType
from pyspark import StorageLevel


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("ais-collision")


# -----------------------------------------------------------------------------
# Configuration. Values can be overridden in docker-compose.yml or docker run -e.
# -----------------------------------------------------------------------------
CENTER_LAT = float(os.environ.get("CENTER_LAT", "55.225000"))
CENTER_LON = float(os.environ.get("CENTER_LON", "14.245000"))
RADIUS_NM = float(os.environ.get("RADIUS_NM", "50"))
RADIUS_KM = RADIUS_NM * 1.852

START_TS = os.environ.get("START_TS", "2021-12-01 00:00:00")
END_TS = os.environ.get("END_TS", "2021-12-31 23:59:59")

DATA_DIR = os.environ.get("DATA_DIR", "/data")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/output")
INPUT_GLOB = os.environ.get("INPUT_GLOB", "aisdk-2021-12-*.csv")

EXCLUDE_RESCUE_VESSELS = os.environ.get("EXCLUDE_RESCUE_VESSELS", "true").lower() == "true"
MAX_RECORD_SOG_KNOTS = float(os.environ.get("MAX_RECORD_SOG_KNOTS", "50"))
MIN_RECORD_SOG_KNOTS = float(os.environ.get("MIN_RECORD_SOG_KNOTS", "0.5"))
MAX_IMPLIED_SPEED_KNOTS = float(os.environ.get("MAX_IMPLIED_SPEED_KNOTS", "80"))
MAX_GAP_FOR_SPEED_CHECK_HOURS = float(os.environ.get("MAX_GAP_FOR_SPEED_CHECK_HOURS", "6"))

COLLISION_DISTANCE_M = float(os.environ.get("COLLISION_DISTANCE_M", "500"))
MIN_COLLISION_DISTANCE_M = float(os.environ.get("MIN_COLLISION_DISTANCE_M", "0.5"))

# Maximum allowed timestamp difference between the two AIS messages being compared.
TIME_TOLERANCE_SECONDS = int(os.environ.get("TIME_TOLERANCE_SECONDS", "120"))

# Plot window around the selected closest approach.
TRAJECTORY_WINDOW_MIN = int(os.environ.get("TRAJECTORY_WINDOW_MIN", "10"))

# Spatial grid cell size used to avoid a full Cartesian join.
SPATIAL_CELL_DEG = float(os.environ.get("SPATIAL_CELL_DEG", "0.01"))

os.makedirs(OUTPUT_DIR, exist_ok=True)


# Bounding box before exact radius filtering.
LAT_DEG = RADIUS_KM / 111.0
LON_DEG = RADIUS_KM / (111.0 * math.cos(math.radians(CENTER_LAT)))
LAT_MIN, LAT_MAX = CENTER_LAT - LAT_DEG, CENTER_LAT + LAT_DEG
LON_MIN, LON_MAX = CENTER_LON - LON_DEG, CENTER_LON + LON_DEG


def create_spark() -> SparkSession:
    """Create a local Spark session suitable for Docker Desktop."""
    return (
        SparkSession.builder
        .appName("AIS-Collision-Detection")
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "10g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "10g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "512"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.local.dir", "/tmp/spark-local")
        .getOrCreate()
    )


def haversine_m_expr(lat1, lon1, lat2, lon2):
    """Spark SQL expression for great-circle distance in metres."""
    r = F.lit(6_371_000.0)
    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dlam = F.radians(lon2 - lon1)
    a = (
        F.pow(F.sin(dphi / 2.0), 2.0)
        + F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(dlam / 2.0), 2.0)
    )
    return 2.0 * r * F.asin(F.sqrt(a))


def safe_col_name(name: str) -> str:
    """Normalize AIS CSV column names.

    Example: '# Timestamp' must become 'Timestamp', not '_Timestamp'.
    """
    cleaned = (
        name.replace("\ufeff", "")
        .replace("#", "")
        .strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
    )
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def load_data(spark: SparkSession) -> DataFrame:
    input_path = os.path.join(DATA_DIR, INPUT_GLOB)
    log.info("Loading CSV input: %s", input_path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .csv(input_path)
    )

    if not df.columns:
        raise RuntimeError(f"No CSV files found at {input_path}")

    for old in df.columns:
        new = safe_col_name(old)
        if old != new:
            df = df.withColumnRenamed(old, new)

    required = {"Timestamp", "MMSI", "Latitude", "Longitude"}
    missing = required.difference(set(df.columns))
    if missing:
        raise RuntimeError(
            f"Input is missing required columns: {sorted(missing)}. "
            f"Columns found after normalization: {df.columns}"
        )

    log.info("Columns found after normalization: %s", df.columns)

    # Some AIS files can have no Name/SOG/COG columns; create them for stable code.
    for col_name in ["Name", "SOG", "COG", "Navigational_status", "Type_of_mobile"]:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast("string"))

    df = (
        df.withColumn("MMSI", F.col("MMSI").cast(LongType()))
          .withColumn("Latitude", F.col("Latitude").cast(DoubleType()))
          .withColumn("Longitude", F.col("Longitude").cast(DoubleType()))
          .withColumn("SOG", F.col("SOG").cast(DoubleType()))
          .withColumn("COG", F.col("COG").cast(DoubleType()))
    )
    return df


def parse_and_filter_time(df: DataFrame) -> DataFrame:
    """Parse common AIS timestamp formats and keep the configured time period."""
    df = df.withColumn(
        "ts",
        F.coalesce(
            F.to_timestamp("Timestamp", "dd/MM/yyyy HH:mm:ss"),
            F.to_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("Timestamp", "yyyy/MM/dd HH:mm:ss"),
        ),
    )

    out = df.filter(
        F.col("ts").isNotNull()
        & (F.col("ts") >= F.lit(START_TS).cast(TimestampType()))
        & (F.col("ts") <= F.lit(END_TS).cast(TimestampType()))
    )
    log.info("Time filter: %s to %s", START_TS, END_TS)
    return out


def geographic_filter(df: DataFrame) -> DataFrame:
    """Cheap bounding box followed by exact 50 nm Haversine radius."""
    df = df.filter(
        F.col("Latitude").between(LAT_MIN, LAT_MAX)
        & F.col("Longitude").between(LON_MIN, LON_MAX)
    )

    df = df.withColumn(
        "distance_from_centre_km",
        haversine_m_expr(F.lit(CENTER_LAT), F.lit(CENTER_LON), F.col("Latitude"), F.col("Longitude")) / 1000.0,
    ).filter(F.col("distance_from_centre_km") <= RADIUS_KM)
    return df


def clean_data(df: DataFrame) -> DataFrame:
    """Remove AIS records that can create false closest-approach events."""
    status_l = F.lower(F.coalesce(F.col("Navigational_status"), F.lit("")))
    type_l = F.lower(F.coalesce(F.col("Type_of_mobile"), F.lit("")))
    name_l = F.lower(F.coalesce(F.col("Name"), F.lit("")))

    df = df.filter(
        F.col("MMSI").isNotNull()
        & F.col("Latitude").isNotNull()
        & F.col("Longitude").isNotNull()
        & F.col("Latitude").between(-90.0, 90.0)
        & F.col("Longitude").between(-180.0, 180.0)
        & ~((F.col("Latitude") == 91.0) | (F.col("Longitude") == 0.0))
        & (F.col("SOG").isNull() | (F.col("SOG") <= MAX_RECORD_SOG_KNOTS))
        & ~status_l.isin("at anchor", "moored", "aground", "not under command")
        & ~type_l.isin("base station", "aton")
    )

    if EXCLUDE_RESCUE_VESSELS:
        df = df.filter(~name_l.contains("rescue"))

    # Keep only records that describe motion. Missing SOG is kept because some AIS
    # messages omit it, but explicit 0.0 kn records are excluded from pair detection.
    df = df.filter(F.col("SOG").isNull() | (F.col("SOG") >= MIN_RECORD_SOG_KNOTS))

    # GPS jump filter based on implied speed between consecutive records of the same MMSI.
    # Long gaps are not treated as jumps, because a vessel can genuinely move many km over hours.
    w = Window.partitionBy("MMSI").orderBy("ts")
    df = (
        df.withColumn("prev_lat", F.lag("Latitude").over(w))
          .withColumn("prev_lon", F.lag("Longitude").over(w))
          .withColumn("prev_ts", F.lag("ts").over(w))
    )

    df = df.withColumn(
        "step_distance_m",
        F.when(
            F.col("prev_lat").isNotNull(),
            haversine_m_expr(F.col("prev_lat"), F.col("prev_lon"), F.col("Latitude"), F.col("Longitude")),
        ).otherwise(F.lit(0.0)),
    ).withColumn(
        "dt_hours",
        (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) / 3600.0,
    ).withColumn(
        "implied_speed_knots",
        F.when(F.col("dt_hours") > 0, (F.col("step_distance_m") / 1852.0) / F.col("dt_hours"))
         .otherwise(F.lit(0.0)),
    )

    df = df.filter(
        F.col("prev_ts").isNull()
        | (F.col("dt_hours") <= 0)
        | (F.col("dt_hours") > MAX_GAP_FOR_SPEED_CHECK_HOURS)
        | (F.col("implied_speed_knots") <= MAX_IMPLIED_SPEED_KNOTS)
    )

    return df.select("MMSI", "Name", "ts", "Latitude", "Longitude", "SOG", "COG")


def add_join_keys(df: DataFrame) -> DataFrame:
    """Add temporal and spatial keys used to avoid a full Cartesian join."""
    # One-minute buckets. Candidate records are compared against neighbouring
    # buckets and then filtered by exact absolute time difference.
    bucket_seconds = 60
    df = df.withColumn(
        "time_bucket",
        (F.floor(F.unix_timestamp("ts") / bucket_seconds) * bucket_seconds).cast(LongType()),
    )

    df = df.withColumn("lat_cell", F.floor(F.col("Latitude") / F.lit(SPATIAL_CELL_DEG)).cast(LongType()))
    df = df.withColumn("lon_cell", F.floor(F.col("Longitude") / F.lit(SPATIAL_CELL_DEG)).cast(LongType()))
    return df


def find_candidates(df: DataFrame) -> DataFrame:
    """Find close moving-vessel pairs using time and spatial partition keys."""
    df_keyed = add_join_keys(df)

    # Expand the left side to neighbouring time/spatial cells, then equi-join.
    a = (
        df_keyed
        .withColumn("join_time_bucket", F.explode(F.array(
            F.col("time_bucket") - 60,
            F.col("time_bucket"),
            F.col("time_bucket") + 60,
        )))
        .withColumn("join_lat_cell", F.explode(F.array(
            F.col("lat_cell") - 1,
            F.col("lat_cell"),
            F.col("lat_cell") + 1,
        )))
        .withColumn("join_lon_cell", F.explode(F.array(
            F.col("lon_cell") - 1,
            F.col("lon_cell"),
            F.col("lon_cell") + 1,
        )))
        .alias("a")
    )
    b = df_keyed.alias("b")

    joined = a.join(
        b,
        on=(
            (F.col("a.join_time_bucket") == F.col("b.time_bucket"))
            & (F.col("a.join_lat_cell") == F.col("b.lat_cell"))
            & (F.col("a.join_lon_cell") == F.col("b.lon_cell"))
            & (F.col("a.MMSI") < F.col("b.MMSI"))
        ),
        how="inner",
    )

    # First check coordinate closeness, then apply the exact time threshold.
    # This makes the logic closer to: "are the vessels physically close, and did this happen at nearly the same time?"

    # Cheap coordinate prefilter before Haversine.
    max_deg = (COLLISION_DISTANCE_M / 1000.0) / 111.0 * 1.5
    joined = joined.filter(
        (F.abs(F.col("a.Latitude") - F.col("b.Latitude")) <= max_deg)
        & (F.abs(F.col("a.Longitude") - F.col("b.Longitude")) <= max_deg)
    )

    # Exact Haversine distance filter.
    joined = joined.withColumn(
        "distance_m",
        haversine_m_expr(
            F.col("a.Latitude"),
            F.col("a.Longitude"),
            F.col("b.Latitude"),
            F.col("b.Longitude")
        ),
    ).filter(
        (F.col("distance_m") <= COLLISION_DISTANCE_M)
        & (F.col("distance_m") >= MIN_COLLISION_DISTANCE_M)
    )

    # Only now apply exact time difference.
    joined = joined.withColumn(
        "time_diff_s",
        F.abs(F.unix_timestamp("a.ts") - F.unix_timestamp("b.ts"))
    ).filter(
        F.col("time_diff_s") <= TIME_TOLERANCE_SECONDS
    )

    # Approximate event timestamp: midpoint between the two AIS timestamps.
    joined = joined.withColumn(
        "event_ts",
        F.to_timestamp(
            F.from_unixtime(
                (
                        (F.unix_timestamp("a.ts") + F.unix_timestamp("b.ts")) / 2
                ).cast(LongType())
            )
        )
    )

    return joined.select(
        F.col("a.MMSI").alias("mmsi_a"),
        F.col("b.MMSI").alias("mmsi_b"),
        F.col("a.Name").alias("name_a"),
        F.col("b.Name").alias("name_b"),
        F.col("a.ts").alias("ts_a"),
        F.col("b.ts").alias("ts_b"),
        F.col("a.Latitude").alias("lat_a"),
        F.col("a.Longitude").alias("lon_a"),
        F.col("b.Latitude").alias("lat_b"),
        F.col("b.Longitude").alias("lon_b"),
        F.col("a.SOG").alias("sog_a"),
        F.col("b.SOG").alias("sog_b"),
        F.col("time_diff_s"),
        F.col("event_ts"),
        F.col("distance_m"),
    ).dropDuplicates(["mmsi_a", "mmsi_b", "ts_a", "ts_b"])

def write_no_result(reason: str) -> None:
    path = os.path.join(OUTPUT_DIR, "collision_results.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("AIS VESSEL COLLISION DETECTION - NO RESULT\n")
        f.write("=" * 55 + "\n")
        f.write(reason.strip() + "\n")
    log.error(reason)
    log.error("Diagnostic file written to %s", path)


def identify_collision(candidates: DataFrame):
    row = (
        candidates
        .orderBy(
            F.col("distance_m").asc(),
            F.col("time_diff_s").asc()
        )
        .limit(1)
        .collect()
    )

    if not row:
        write_no_result(
            "No vessel pair was found inside the configured distance/time thresholds."
        )
        sys.exit(2)

    return row[0]


def extract_trajectories(df: DataFrame, collision):
    collision_ts = collision["ts_a"]
    start = collision_ts - timedelta(minutes=TRAJECTORY_WINDOW_MIN)
    end = collision_ts + timedelta(minutes=TRAJECTORY_WINDOW_MIN)

    traj = (
        df.filter(
            F.col("MMSI").isin([int(collision["mmsi_a"]), int(collision["mmsi_b"])])
            & (F.col("ts") >= F.lit(start).cast(TimestampType()))
            & (F.col("ts") <= F.lit(end).cast(TimestampType()))
        )
        .orderBy("MMSI", "ts")
        .toPandas()
    )

    return (
        traj[traj["MMSI"] == collision["mmsi_a"]].reset_index(drop=True),
        traj[traj["MMSI"] == collision["mmsi_b"]].reset_index(drop=True),
    )


def clean_name(name, mmsi) -> str:
    if name is None:
        return f"MMSI {mmsi}"
    s = str(name).strip()
    if not s or s.lower() in {"unknown", "nan", "none"}:
        return f"MMSI {mmsi}"
    return s


def plot_trajectories(traj_a, traj_b, collision) -> str:
    mmsi_a, mmsi_b = collision["mmsi_a"], collision["mmsi_b"]
    name_a = clean_name(collision["name_a"], mmsi_a)
    name_b = clean_name(collision["name_b"], mmsi_b)
    col_ts = collision["ts_a"]
    col_lat = (collision["lat_a"] + collision["lat_b"]) / 2.0
    col_lon = (collision["lon_a"] + collision["lon_b"]) / 2.0

    fig, ax = plt.subplots(figsize=(10, 8))

    def draw(traj, label, marker):
        if traj.empty:
            return
        ax.plot(traj["Longitude"], traj["Latitude"], marker=marker, linewidth=2, label=label)
        ax.scatter(traj.iloc[0]["Longitude"], traj.iloc[0]["Latitude"], s=90, marker="^", label=f"{label} start")
        ax.scatter(traj.iloc[-1]["Longitude"], traj.iloc[-1]["Latitude"], s=90, marker="v", label=f"{label} end")

        # Annotate a few relative times so the ±10 min window is clear.
        ref = np.datetime64(col_ts)
        step = max(1, len(traj) // 4)
        for i in range(0, len(traj), step):
            row = traj.iloc[i]
            offset = float((np.datetime64(row["ts"]) - ref) / np.timedelta64(1, "m"))
            ax.annotate(f"{offset:+.0f} min", (row["Longitude"], row["Latitude"]), fontsize=8)

    draw(traj_a, f"{name_a} ({mmsi_a})", "o")
    draw(traj_b, f"{name_b} ({mmsi_b})", "s")

    ax.scatter(col_lon, col_lat, s=250, marker="*", label="closest approach")
    ax.set_xlabel("Longitude (degrees East)")
    ax.set_ylabel("Latitude (degrees North)")
    ax.set_title("AIS trajectories: 10 min before and 10 min after closest approach")
    ax.grid(True, alpha=0.4)
    ax.legend(fontsize=8, loc="best")
    ax.set_aspect("equal", adjustable="datalim")

    info = (
        f"Time: {col_ts}\n"
        f"Position: {col_lat:.6f}, {col_lon:.6f}\n"
        f"Distance: {collision['distance_m']:.2f} m\n"
        f"Time difference: {collision['time_diff_s']} s"
    )
    ax.text(0.02, 0.02, info, transform=ax.transAxes, fontsize=9,
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.85))

    out = os.path.join(OUTPUT_DIR, "collision_trajectory.png")
    plt.tight_layout()
    plt.savefig(out, dpi=180)
    plt.close(fig)
    return out


def write_results(collision) -> str:
    name_a = clean_name(collision["name_a"], collision["mmsi_a"])
    name_b = clean_name(collision["name_b"], collision["mmsi_b"])
    col_lat = (collision["lat_a"] + collision["lat_b"]) / 2.0
    col_lon = (collision["lon_a"] + collision["lon_b"]) / 2.0

    text = f"""AIS VESSEL COLLISION DETECTION - RESULTS
=======================================================
Vessel A MMSI        : {collision['mmsi_a']}
Vessel A Name        : {name_a}
Vessel B MMSI        : {collision['mmsi_b']}
Vessel B Name        : {name_b}
Timestamp A          : {collision['ts_a']}
Timestamp B          : {collision['ts_b']}
Time difference      : {collision['time_diff_s']} s
Collision latitude   : {col_lat:.6f}
Collision longitude  : {col_lon:.6f}
Closest approach     : {collision['distance_m']:.2f} m
SOG A                : {collision['sog_a']} kn
SOG B                : {collision['sog_b']} kn
=======================================================
"""
    out = os.path.join(OUTPUT_DIR, "collision_results.txt")
    with open(out, "w", encoding="utf-8") as f:
        f.write(text)
    log.info("\n%s", text)
    return out


def count_or_stop(df: DataFrame, label: str) -> None:
    n = df.count()
    log.info("%s: %s rows", label, f"{n:,}")
    if n == 0:
        write_no_result(
            f"No rows left after stage: {label}.\n"
            f"Input path: {os.path.join(DATA_DIR, INPUT_GLOB)}\n"
            f"Configured time window: {START_TS} to {END_TS}\n"
            f"Configured area: centre=({CENTER_LAT}, {CENTER_LON}), radius={RADIUS_NM} nm\n"
            "This usually means the CSV file is from a different date or does not cover the assignment area."
        )
        sys.exit(2)


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Step 1/7 - load CSV")
    raw = load_data(spark)
    count_or_stop(raw, "raw input")

    log.info("Step 2/7 - parse timestamp and filter assignment period")
    timed = parse_and_filter_time(raw)
    count_or_stop(timed, "after time filter")

    log.info("Step 3/7 - filter 50 nautical mile study area")
    geo = geographic_filter(timed)
    count_or_stop(geo, "after geographic filter")

    log.info("Step 4/7 - clean moving vessels and remove GPS noise")
    clean = clean_data(geo)
    count_or_stop(clean, "after cleaning")

    log.info("Step 5/7 - generate time/spatial candidates")
    candidates = find_candidates(clean)
    log.info("Candidate dataframe created. Selecting closest event...")

    log.info("Step 6/7 - select closest event")
    collision = identify_collision(candidates)

    log.info("Step 7/7 - plot trajectories and write outputs")
    traj_a, traj_b = extract_trajectories(clean, collision)
    plot_path = plot_trajectories(traj_a, traj_b, collision)
    result_path = write_results(collision)

    spark.stop()
    log.info("Done. Results: %s ; %s", result_path, plot_path)


if __name__ == "__main__":
    main()
