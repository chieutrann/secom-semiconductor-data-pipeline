"""
Silver Layer: Schema validation, label conversion, timestamp parsing, and
data quality profiling for the SECOM dataset.

Transformations:
  1. silver_secom_validated — Parse timestamp, convert labels (-1→Pass, +1→Fail),
     flag invalid records, count missing sensors per row. No data is dropped.
  2. silver_sensor_profile — Per-sensor statistics: null count/pct, min/max/mean/stddev,
     constant detection, all-null detection across 590 sensors.
  3. silver_secom_cleaned — Wide-format cleaned data: invalid records filtered out,
     all 590 sensor nulls imputed with per-sensor mean.
  4. silver_secom_cleaned_long — Long-format analysis-ready data: invalid records
     filtered, uninformative sensors (constant / all-null / >50% missing) removed,
     remaining nulls imputed with per-sensor mean.
"""

from pyspark.sql import functions as F
from pyspark import pipelines as dp

# 590 sensor column names (sensor_001 through sensor_590)
SENSOR_COLS = [f"sensor_{i:03d}" for i in range(1, 591)]


@dp.materialized_view(
    name="silver_secom_validated",
    comment="Silver layer: validated SECOM data with parsed timestamp, converted labels, and quality flags. All rows retained — no blind dropping."
)
@dp.expect("source_row_id_not_null", "source_row_id IS NOT NULL")
def silver_secom_validated():
    """Validate, clean, and enrich bronze SECOM data without dropping any rows."""
    df = spark.read.table("bronze_secom_raw")

    # Parse timestamp string (dd/MM/yyyy HH:mm:ss) into a TIMESTAMP type
    df = df.withColumn(
        "measurement_timestamp",
        F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"),
    )
    df = df.withColumn(
        "is_valid_timestamp",
        F.col("measurement_timestamp").isNotNull(),
    )

    # Convert label: -1 = Pass, +1 = Fail, anything else = Unknown
    df = df.withColumn(
        "quality_result",
        F.when(F.col("label_raw") == -1, "Pass")
         .when(F.col("label_raw") == 1, "Fail")
         .otherwise("Unknown"),
    )
    df = df.withColumn(
        "is_valid_label",
        F.col("quality_result") != "Unknown",
    )

    # Count missing sensor values per row (sum of null indicators across 590 sensors)
    null_indicators = [F.col(c).isNull().cast("int") for c in SENSOR_COLS]
    df = df.withColumn("missing_sensor_count", sum(null_indicators))
    df = df.withColumn(
        "has_missing_sensors",
        F.col("missing_sensor_count") > 0,
    )

    # Overall record validity flag
    df = df.withColumn(
        "is_valid_record",
        F.col("is_valid_timestamp") & F.col("is_valid_label"),
    )

    return df


@dp.materialized_view(
    name="silver_sensor_profile",
    comment="Silver layer: per-sensor profiling across 590 sensors — null count/pct, min/max/mean/stddev, constant and all-null detection."
)
def silver_sensor_profile():
    """Profile each of the 590 sensors for null rate, statistics, and constant detection."""
    df = spark.read.table("bronze_secom_raw")

    # Unpivot 590 sensor columns into long format for per-sensor aggregation
    long_df = df.unpivot(
        ids=["source_row_id"],
        values=SENSOR_COLS,
        variableColumnName="sensor_name",
        valueColumnName="sensor_value",
    )

    return (
        long_df.groupBy("sensor_name")
        .agg(
            F.count("*").alias("total_rows"),
            F.count("sensor_value").alias("non_null_count"),
            F.sum(F.col("sensor_value").isNull().cast("int")).alias("null_count"),
            F.min("sensor_value").alias("min_value"),
            F.max("sensor_value").alias("max_value"),
            F.mean("sensor_value").alias("mean_value"),
            F.stddev("sensor_value").alias("stddev_value"),
        )
        .withColumn(
            "null_pct",
            F.round(F.col("null_count") / F.col("total_rows") * 100, 2),
        )
        .withColumn(
            "is_constant",
            (F.col("stddev_value").isNull()) | (F.col("stddev_value") == 0),
        )
        .withColumn(
            "is_all_null",
            F.col("non_null_count") == 0,
        )
        .orderBy("sensor_name")
    )


# Non-sensor columns preserved in cleaned datasets
KEEP_COLS = [
    "source_row_id",
    "label_raw",
    "quality_result",
    "measurement_timestamp",
    "missing_sensor_count",
]


@dp.materialized_view(
    name="silver_secom_cleaned",
    comment="Silver layer: wide-format cleaned SECOM data — invalid records removed, all 590 sensor nulls imputed with per-sensor mean."
)
@dp.expect("source_row_id_not_null", "source_row_id IS NOT NULL")
def silver_secom_cleaned():
    """Filter invalid records and impute missing sensor values with per-sensor mean (wide format)."""
    validated = spark.read.table("silver_secom_validated")

    # Step 1: Keep only valid records (valid timestamp AND valid label)
    valid_df = validated.filter(F.col("is_valid_record"))

    # Step 2: Compute per-sensor mean across all valid rows (single-row DataFrame)
    mean_exprs = [F.mean(c).alias(c) for c in SENSOR_COLS]
    means_row = valid_df.agg(*mean_exprs)

    # Step 3: Cross-join with means (broadcast — single row) for imputation
    joined = valid_df.select(*KEEP_COLS, *SENSOR_COLS).crossJoin(
        means_row.select(*[F.col(c).alias(f"_mean_{c}") for c in SENSOR_COLS])
    )

    # Step 4: Coalesce each sensor with its mean (select-based to avoid column
    # resolution issues — missing_sensor_count references original sensor cols)
    imputed_exprs = [
        F.coalesce(F.col(c), F.col(f"_mean_{c}")).alias(c) for c in SENSOR_COLS
    ]

    return joined.select(
        F.col("source_row_id"),
        F.col("label_raw"),
        F.col("quality_result"),
        F.col("measurement_timestamp"),
        F.col("missing_sensor_count").alias("imputed_sensor_count"),
        *imputed_exprs,
    )


@dp.materialized_view(
    name="silver_secom_cleaned_long",
    comment="Silver layer: long-format analysis-ready SECOM data — uninformative sensors removed, nulls imputed with mean."
)
def silver_secom_cleaned_long():
    """Unpivot sensors to long format, drop uninformative sensors, impute nulls with per-sensor mean."""
    validated = spark.read.table("silver_secom_validated")
    profile = spark.read.table("silver_sensor_profile")

    # Step 1: Keep only valid records
    valid_df = validated.filter(F.col("is_valid_record"))

    # Step 2: Identify good sensors (not constant, not all-null, not high-missing)
    good_sensors = profile.filter(
        ~F.col("is_constant")
        & ~F.col("is_all_null")
        & (F.col("null_pct") <= 50)
    ).select("sensor_name")

    # Step 3: Unpivot 590 sensors to long format
    long_df = valid_df.select(*KEEP_COLS, *SENSOR_COLS).unpivot(
        ids=KEEP_COLS,
        values=SENSOR_COLS,
        variableColumnName="sensor_name",
        valueColumnName="sensor_value",
    )

    # Step 4: Keep only good sensors (inner join filters out bad ones)
    long_df = long_df.join(good_sensors, on="sensor_name", how="inner")

    # Step 5: Impute remaining nulls with per-sensor mean
    sensor_means = long_df.groupBy("sensor_name").agg(
        F.mean("sensor_value").alias("sensor_mean")
    )
    long_df = long_df.join(sensor_means, on="sensor_name", how="left")
    long_df = long_df.withColumn(
        "sensor_value",
        F.coalesce(F.col("sensor_value"), F.col("sensor_mean")),
    ).drop("sensor_mean")

    # Step 6: Add imputation flag and rename for clarity
    long_df = long_df.withColumn(
        "was_imputed",
        F.col("missing_sensor_count") > 0,
    ).withColumnRenamed("missing_sensor_count", "original_missing_sensor_count")

    return long_df
