"""Helper functions for the group assignment task files."""

# Copyright 2025 Tampere University
# This notebook and software was developed for a Tampere University course COMP.CS.320.
# This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
# Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


BASIC: str = "Basic"
ADVANCED: str = "Advanced"


# Helper functions to separate the task outputs from each other
def print_task_line(taskType: str, taskNumber: int = 0, subType: str = "") -> None:
    task_title: str = (
        f"{taskType} Task {taskNumber} - {subType}" if subType != "" and taskNumber > 0
        else f"{taskType} Task {taskNumber}" if taskNumber > 0
        else f"{taskType}"
    )
    separating_line: str = "=" * len(task_title)
    print(f"{separating_line}\n{task_title}\n{separating_line}")


# Get a Spark session that works for all basic and advanced tasks
def get_spark_session() -> SparkSession:
    builder = SparkSession.builder \
        .appName("assignment-python") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("WARN")

    return spark
