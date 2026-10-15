"""
Gold Layer: Quality monitoring tables for the SECOM semiconductor dataset.

Eight monitoring tables:
  1. gold_pass_fail_summary      — overall pass/fail counts, rates, and overall missing rate
  2. gold_sensor_completeness    — per-sensor completeness with quality categories
  3. gold_data_quality_issues    — summary of all detected data quality issues
  4. gold_sensor_pass_fail_stats — per-sensor mean/stddev broken down by Pass vs Fail
  5. gold_quality_trend          — daily pass/fail rate trend over time
  6. gold_completeness_summary   — aggregate completeness counts with overall missing rate
  7. gold_cleaning_summary       — records filtered, sensors dropped, values imputed
  8. gold_sensor_ranking         — sensors ranked by discriminative power (effect size)
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import pipelines as dp

# 590 sensor column names (sensor_001 through sensor_590)
SENSOR_COLS = [f"sensor_{i:03d}" for i in range(1, 591)]


@dp.materialized_view(
    name="gold_pass_fail_summary",
    comment="Gold layer: pass/fail rate summary and overall missing rate for SECOM quality monitoring."
)
def gold_pass_fail_summary():
    """Aggregate pass/fail rates and overall missing rate from validated silver data."""
    df = spark.read.table("silver_secom_validated")
    profile = spark.read.table("silver_sensor_profile")

    # Overall missing rate across all sensor cells
    missing_rate = profile.agg(
        F.round(F.sum("null_count") / F.sum("total_rows") * 100, 2).alias("overall_missing_rate_pct")
    )

    return (
        df.agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("quality_result") == "Pass", 1).otherwise(0)).alias("pass_count"),
            F.sum(F.when(F.col("quality_result") == "Fail", 1).otherwise(0)).alias("fail_count"),
            F.sum(F.when(F.col("quality_result") == "Unknown", 1).otherwise(0)).alias("unknown_label_count"),
            F.sum((~F.col("is_valid_timestamp")).cast("int")).alias("invalid_timestamp_count"),
        )
        .withColumn("pass_rate_pct", F.round(F.col("pass_count") / F.col("total_records") * 100, 2))
        .withColumn("fail_rate_pct", F.round(F.col("fail_count") / F.col("total_records") * 100, 2))
        .withColumn("fail_pass_ratio", F.round(F.col("fail_count") / F.col("pass_count"), 4))
        .crossJoin(missing_rate)
    )


@dp.materialized_view(
    name="gold_sensor_completeness",
    comment="Gold layer: per-sensor completeness metrics with quality categories."
)
def gold_sensor_completeness():
    """Compute completeness metrics per sensor from the silver sensor profile."""
    df = spark.read.table("silver_sensor_profile")

    return (
        df.select(
            "sensor_name",
            "total_rows",
            "non_null_count",
            "null_count",
            "null_pct",
            "is_constant",
            "is_all_null",
        )
        .withColumn(
            "completeness_pct",
            F.round(F.col("non_null_count") / F.col("total_rows") * 100, 2),
        )
        .withColumn(
            "completeness_category",
            F.when(F.col("completeness_pct") >= 95, "High (>=95%)")
             .when(F.col("completeness_pct") >= 80, "Medium (80-95%)")
             .when(F.col("completeness_pct") >= 50, "Low (50-80%)")
             .otherwise("Critical (<50%)"),
        )
        .orderBy(F.col("null_pct").desc())
    )


@dp.materialized_view(
    name="gold_data_quality_issues",
    comment="Gold layer: summary of all data quality issues detected in the SECOM dataset."
)
def gold_data_quality_issues():
    """Summarize all data quality issues: invalid timestamps, unknown labels, constant sensors, all-null sensors, high-missing sensors, rows with missing data."""
    validated = spark.read.table("silver_secom_validated")
    profile = spark.read.table("silver_sensor_profile")

    # Aggregate all metrics into a single row via cross join
    metrics = (
        validated.agg(
            F.sum((~F.col("is_valid_timestamp")).cast("int")).alias("invalid_timestamps"),
            F.sum((F.col("quality_result") == "Unknown").cast("int")).alias("unknown_labels"),
            F.sum(F.col("has_missing_sensors").cast("int")).alias("rows_with_missing_sensors"),
        )
        .crossJoin(
            profile.agg(
                F.sum(F.col("is_constant").cast("int")).alias("constant_sensors"),
                F.sum(F.col("is_all_null").cast("int")).alias("all_null_sensors"),
                F.sum((F.col("null_pct") > 50).cast("int")).alias("high_missing_sensors"),
            )
        )
    )

    # Build a multi-row issue summary using array of structs + explode
    return (
        metrics.select(
            F.explode(F.array(
                F.struct(
                    F.lit("Invalid Timestamps").alias("issue_type"),
                    F.lit("Warning").alias("severity"),
                    F.col("invalid_timestamps").alias("count"),
                    F.lit("Records where the measurement timestamp could not be parsed.").alias("description"),
                ),
                F.struct(
                    F.lit("Unknown Labels").alias("issue_type"),
                    F.lit("Warning").alias("severity"),
                    F.col("unknown_labels").alias("count"),
                    F.lit("Records with label_raw outside the expected {-1, +1} set.").alias("description"),
                ),
                F.struct(
                    F.lit("Rows with Missing Sensors").alias("issue_type"),
                    F.lit("Info").alias("severity"),
                    F.col("rows_with_missing_sensors").alias("count"),
                    F.lit("Records with at least one null sensor reading.").alias("description"),
                ),
                F.struct(
                    F.lit("Constant Sensors").alias("issue_type"),
                    F.lit("Info").alias("severity"),
                    F.col("constant_sensors").alias("count"),
                    F.lit("Sensors with zero variance (stddev = 0 or NULL) — no information content.").alias("description"),
                ),
                F.struct(
                    F.lit("All-Null Sensors").alias("issue_type"),
                    F.lit("Critical").alias("severity"),
                    F.col("all_null_sensors").alias("count"),
                    F.lit("Sensors with no non-null values — completely missing data.").alias("description"),
                ),
                F.struct(
                    F.lit("High-Missing Sensors (>50%)").alias("issue_type"),
                    F.lit("Warning").alias("severity"),
                    F.col("high_missing_sensors").alias("count"),
                    F.lit("Sensors where more than 50% of rows have null values.").alias("description"),
                ),
            )).alias("issue")
        )
        .select("issue.*")
        .orderBy(F.col("count").desc())
    )


@dp.materialized_view(
    name="gold_sensor_pass_fail_stats",
    comment="Gold layer: per-sensor statistics broken down by Pass vs Fail quality result."
)
def gold_sensor_pass_fail_stats():
    """For each sensor, compute mean/stddev for Pass vs Fail rows, mean difference, and effect size (Cohen's d)."""
    validated = spark.read.table("silver_secom_validated")

    # Unpivot sensors to long format, keeping quality_result for grouping
    long_df = validated.select(
        "source_row_id", "quality_result", *SENSOR_COLS
    ).unpivot(
        ids=["source_row_id", "quality_result"],
        values=SENSOR_COLS,
        variableColumnName="sensor_name",
        valueColumnName="sensor_value",
    )

    # Per-sensor per-result statistics
    stats = long_df.groupBy("sensor_name", "quality_result").agg(
        F.mean("sensor_value").alias("mean_value"),
        F.stddev("sensor_value").alias("stddev_value"),
        F.count("sensor_value").alias("non_null_count"),
    )

    # Split Pass and Fail stats, then join side by side
    pass_stats = (
        stats.filter(F.col("quality_result") == "Pass")
        .select(
            "sensor_name",
            F.col("mean_value").alias("mean_pass"),
            F.col("stddev_value").alias("stddev_pass"),
            F.col("non_null_count").alias("count_pass"),
        )
    )
    fail_stats = (
        stats.filter(F.col("quality_result") == "Fail")
        .select(
            "sensor_name",
            F.col("mean_value").alias("mean_fail"),
            F.col("stddev_value").alias("stddev_fail"),
            F.col("non_null_count").alias("count_fail"),
        )
    )

    result = pass_stats.join(fail_stats, on="sensor_name", how="outer")

    # Mean difference and Cohen's d effect size (normalized by pooled stddev)
    result = (
        result
        .withColumn("mean_difference", F.col("mean_fail") - F.col("mean_pass"))
        .withColumn(
            "pooled_stddev",
            F.sqrt(
                (F.col("stddev_pass") ** 2 * F.col("count_pass")
                 + F.col("stddev_fail") ** 2 * F.col("count_fail"))
                / (F.col("count_pass") + F.col("count_fail"))
            ),
        )
        .withColumn(
            "effect_size",
            F.when(
                F.col("pooled_stddev").isNotNull() & (F.col("pooled_stddev") > 0),
                F.abs(F.col("mean_difference") / F.col("pooled_stddev")),
            ).otherwise(F.lit(None)),
        )
    )

    return result.orderBy(F.col("effect_size").desc())


@dp.materialized_view(
    name="gold_quality_trend",
    comment="Gold layer: daily pass/fail rate trend over time."
)
def gold_quality_trend():
    """Daily pass/fail counts and rates from validated data with parsed timestamps."""
    validated = spark.read.table("silver_secom_validated")

    return (
        validated
        .filter(F.col("is_valid_timestamp"))
        .groupBy(F.to_date("measurement_timestamp").alias("run_date"))
        .agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("quality_result") == "Pass", 1).otherwise(0)).alias("pass_count"),
            F.sum(F.when(F.col("quality_result") == "Fail", 1).otherwise(0)).alias("fail_count"),
        )
        .withColumn("pass_rate_pct", F.round(F.col("pass_count") / F.col("total_records") * 100, 2))
        .withColumn("fail_rate_pct", F.round(F.col("fail_count") / F.col("total_records") * 100, 2))
        .orderBy("run_date")
    )


@dp.materialized_view(
    name="gold_completeness_summary",
    comment="Gold layer: aggregate completeness summary with overall missing rate and category counts."
)
def gold_completeness_summary():
    """Aggregate sensor completeness by category and compute overall missing rate."""
    profile = spark.read.table("silver_sensor_profile")

    completeness_ratio = F.col("non_null_count") / F.col("total_rows")

    return (
        profile.agg(
            F.count("*").alias("total_sensors"),
            F.sum("total_rows").alias("total_cells"),
            F.sum("null_count").alias("total_null_cells"),
            F.sum(F.col("is_constant").cast("int")).alias("constant_sensors"),
            F.sum(F.col("is_all_null").cast("int")).alias("all_null_sensors"),
            F.sum(F.when(completeness_ratio >= 0.95, 1).otherwise(0)).alias("high_completeness_sensors"),
            F.sum(F.when((completeness_ratio >= 0.80) & (completeness_ratio < 0.95), 1).otherwise(0)).alias("medium_completeness_sensors"),
            F.sum(F.when((completeness_ratio >= 0.50) & (completeness_ratio < 0.80), 1).otherwise(0)).alias("low_completeness_sensors"),
            F.sum(F.when(completeness_ratio < 0.50, 1).otherwise(0)).alias("critical_completeness_sensors"),
        )
        .withColumn("overall_missing_rate_pct", F.round(F.col("total_null_cells") / F.col("total_cells") * 100, 2))
        .withColumn("overall_completeness_pct", F.round((1 - F.col("total_null_cells") / F.col("total_cells")) * 100, 2))
    )


@dp.materialized_view(
    name="gold_cleaning_summary",
    comment="Gold layer: summary of data cleaning impact — records filtered, sensors dropped, values imputed."
)
def gold_cleaning_summary():
    """Summarize what the cleaning step changed: records filtered, sensors kept/dropped, values imputed."""
    validated = spark.read.table("silver_secom_validated")
    profile = spark.read.table("silver_sensor_profile")

    is_good_sensor = ~F.col("is_constant") & ~F.col("is_all_null") & (F.col("null_pct") <= 50)

    validated_metrics = validated.agg(
        F.count("*").alias("total_records"),
        F.sum(F.col("is_valid_record").cast("int")).alias("valid_records"),
        F.sum((~F.col("is_valid_record")).cast("int")).alias("invalid_records"),
        F.sum("missing_sensor_count").alias("total_missing_values"),
    )

    profile_metrics = profile.agg(
        F.count("*").alias("total_sensors"),
        F.sum(is_good_sensor.cast("int")).alias("good_sensors"),
        F.sum((~is_good_sensor).cast("int")).alias("dropped_sensors"),
    )

    return (
        validated_metrics.crossJoin(profile_metrics)
        .withColumn("records_filtered_pct", F.round(F.col("invalid_records") / F.col("total_records") * 100, 2))
        .withColumn("sensors_dropped_pct", F.round(F.col("dropped_sensors") / F.col("total_sensors") * 100, 2))
    )


@dp.materialized_view(
    name="gold_sensor_ranking",
    comment="Gold layer: sensors ranked by discriminative power between Pass and Fail (Cohen's d effect size)."
)
def gold_sensor_ranking():
    """Rank sensors by effect size to identify the most discriminative features for pass/fail prediction."""
    stats = spark.read.table("gold_sensor_pass_fail_stats")

    w = Window.orderBy(F.col("effect_size").desc())

    return (
        stats
        .filter(F.col("effect_size").isNotNull())
        .select(
            "sensor_name",
            "mean_pass",
            "mean_fail",
            "mean_difference",
            "effect_size",
            "count_pass",
            "count_fail",
        )
        .withColumn("rank", F.row_number().over(w))
        .orderBy("rank")
    )
