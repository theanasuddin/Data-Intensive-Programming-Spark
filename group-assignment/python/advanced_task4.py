"""The advanced task 4 of the group assignment"""

# Copyright 2025 Tampere University
# This notebook and software was developed for a Tampere University course COMP.CS.320.
# This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
# Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

from pyspark.sql import DataFrame, SparkSession

from task_helpers import ADVANCED, get_spark_session, print_task_line


# Change this to True when you have done some implementation of this task
tasks_implemented: bool = False


def run():
    """Run the advanced task 4."""

    # COMP.CS.320 Data-Intensive Programming, Group assignment, Advanced Task 4
    #
    # The instructions for the advanced task 4 of the group assignment are given in
    # the markdown file "assignment-advanced-4-tasks.md" at the root of the repository.
    # This file contains only the starting code not the instructions.
    #
    # A markdown file "assignment-example-outputs-advanced-4.md" at the root of the repository contains example outputs for the tasks.
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



    print_task_line(ADVANCED, 4, "Case 1")
    # Advanced Task 4 - Case 1 - Predicting the hour of the day
    #
    # It is likely that even using the same seed value, the results will be different compared to the example output.

    case1Accuracy: float = __MISSING__IMPLEMENTATION__

    case1AvgHourDiff: float = __MISSING__IMPLEMENTATION__


    print(f"The overall accuracy of the hour prediction model: {round(case1Accuracy*100, 2)} %")
    print(f"The average hour difference between the predicted and actual hour of the day: {case1AvgHourDiff}")



    print_task_line(ADVANCED, 4, "Case 2")
    # Advanced Task 4 - Case 2 - Predicting whether it is a weekend or not

    case2Accuracy: float = __MISSING__IMPLEMENTATION__

    case2AccuracyDF: DataFrame = __MISSING__IMPLEMENTATION__


    print(f"The overall accuracy of the weekend prediction model is {round(case2Accuracy*100, 2)} %")
    print("Accuracy (in percentages) of the weekend predictions based on the day of the week:")
    case2AccuracyDF.show(truncate=False)



    print_task_line(ADVANCED, 4, "Case 3")
    # Advanced Task 4 - Case 3 - Predicting whether it is a weekend or not

    case3Accuracy: float = __MISSING__IMPLEMENTATION__

    case3AccuracyDF: DataFrame = __MISSING__IMPLEMENTATION__


    print(f"The overall accuracy of the device type prediction model is {round(case3Accuracy*100, 2)} %")
    print("Accuracy (in percentages) of the device predictions based on the device:")
    case3AccuracyDF.show(truncate=False)



    print_task_line(ADVANCED, 4, "Case 4")
    # Advanced Task 4 - Case 4 - Own predictions

    # ???
