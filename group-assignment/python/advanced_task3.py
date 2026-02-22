"""The advanced task 3 of the group assignment"""

# Copyright 2025 Tampere University
# This notebook and software was developed for a Tampere University course COMP.CS.320.
# This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
# Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

from glob import glob
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from task_helpers import ADVANCED, get_spark_session, print_task_line


# Change this to True when you have done some implementation of this task
tasks_implemented: bool = False


def run():
    """Run the advanced task 3."""

    # COMP.CS.320 Data-Intensive Programming, Group assignment, Advanced Task 3
    #
    # The instructions for the advanced task 3 of the group assignment are given in
    # the markdown file "assignment-advanced-3-tasks.md" at the root of the repository.
    # This file contains only the starting code not the instructions.
    #
    # A markdown file "assignment-example-outputs-advanced-3.md" at the root of the repository contains example outputs for the tasks.
    #
    # The tasks that can be done in either Scala or Python.
    # This is the Python version intended for local development.
    #
    # For the local development the source data for the tasks can be located in the data folder at the root of the repository.
    # So, instead of accessing the data from the Azure storage container, use the local files as the source.
    #
    # Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    #
    # Don't forget to submit your solutions to Moodle once your group is finished with the assignment.

    spark: SparkSession = get_spark_session()


    # returns a list of existing subdirectories under the input path
    def getDirectoryList(path: str) -> list[str]:
        return sorted([
            name
            for name in glob(f"{path}/*") + glob(f"{path}/.*")
            if Path(name).is_dir()
        ])

    # remove all files and folders from the target path
    def cleanTargetFolder(path: str) -> None:
        for file_path in [
            Path(filename)
            for filename in glob(f"{path}/*") + glob(f"{path}/.*")
        ]:
            if file_path.is_dir():
                cleanTargetFolder(str(file_path))
                file_path.rmdir()
            else:
                file_path.unlink()




    print_task_line(ADVANCED, 3, "Phase 1")
    # Advanced Task 3 - Phase 1 - Loading the data
    #
    # For the local version, the goal is to write the combined data in Delta format to local folder "data/transactions-delta".
    # Depending on your localization settings, some of the timestamps might be printed differently than in the example output.

    targetPath: str = "data/transactions-delta"
    # this will remove all the files from the target path, i.e., a fresh start
    cleanTargetFolder(targetPath)


    __MISSING__IMPLEMENTATION__


    # test code for phase 1
    transaction_ids: list[int] = [
        15471290, 15540933, 15614378, 15683708, 15743561, 15813630, 15887875, 15958050,
        16027329, 16097021, 16173489, 16243958, 16313703, 16384244, 16459459, 16529507,
        16605087, 16675317, 16745233, 16815275, 16890288, 16960180, 17030940, 17101718
    ]

    phase1TestDF: DataFrame = spark.read.format("delta").load(targetPath)
    print(f"Total number of transactions: {phase1TestDF.count()}")
    print("Example transactions:")
    phase1TestDF \
        .filter(F.col("transaction_id").isin(*transaction_ids)) \
        .orderBy(F.col("transaction_id").asc()) \
        .limit(24).show(24, False)



    print_task_line(ADVANCED, 3, "Phase 2")
    # Advanced Task 3 - Phase 2 - Updating the data

    __MISSING__IMPLEMENTATION__


    # test code for phase 2
    phase2TestDF: DataFrame = spark.read.format("delta").load(targetPath) \
        .select("transaction_id", "timestamp", "client_id", "amount_dollars", "merchant_id", "merchant_city", "merchant_state", "merchant_country")

    print(f"Total number of transactions: {phase2TestDF.count()}")
    print("Example transactions:")
    phase2TestDF \
        .filter(F.col("transaction_id").isin(*transaction_ids)) \
        .orderBy(F.col("transaction_id").asc()) \
        .show(24, False)



    print_task_line(ADVANCED, 3, "Phase 3")
    # Advanced Task 3 - Phase 3 - Data calculations

    usMerchantsDF: DataFrame = __MISSING__IMPLEMENTATION__

    nonUSMerchantsDF: DataFrame = __MISSING__IMPLEMENTATION__

    franceMerchantsDF: DataFrame = __MISSING__IMPLEMENTATION__


    print("Top 10 merchants selling in the US:")
    usMerchantsDF.show(truncate=False)

    print("Top 10 merchants selling outside the US:")
    nonUSMerchantsDF.show(truncate=False)

    print("The merchants having a single transaction in December 2015 in France:")
    franceMerchantsDF.show(truncate=False)
