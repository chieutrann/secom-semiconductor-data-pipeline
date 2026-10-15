"""
Bronze Layer: Raw ingestion of the UCI SECOM semiconductor manufacturing dataset.

Reads from the source table workspace.secom.secom_raw_combined, preserving all
raw sensor values (sensor_001–sensor_590), the quality label (label_raw), and
the measurement timestamp. Adds an ingestion timestamp for lineage tracking.

Source: UCI SECOM Dataset
  - 1,567 rows, 590 sensor features, 1 label column, 1 timestamp column
  - label_raw: -1 = Pass, +1 = Fail
  - timestamp format: dd/MM/yyyy HH:mm:ss
"""

from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.materialized_view(
    name="bronze_secom_raw",
    comment="Bronze layer: raw SECOM data — all 590 sensors, label_raw, and timestamp preserved as-is from source."
)
@dp.expect("source_row_id_present", "source_row_id IS NOT NULL")
@dp.expect("label_present", "label_raw IS NOT NULL")
def bronze_secom_raw():
    """Read raw SECOM data from the source table, preserving all columns."""
    return (
        spark.read.table("workspace.secom.secom_raw_combined")
        .withColumn("_ingested_at", F.current_timestamp())
    )
