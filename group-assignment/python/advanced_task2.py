"""The advanced task 2 of the group assignment"""

# Copyright 2025 Tampere University
# This notebook and software was developed for a Tampere University course COMP.CS.320.
# This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
# Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

from pyspark.sql import DataFrame, SparkSession

from task_helpers import ADVANCED, get_spark_session, print_task_line


# Change this to True when you have done some implementation of this task
tasks_implemented: bool = False


def run():
    """Run the advanced task 2."""

    # COMP.CS.320 Data-Intensive Programming, Group assignment, Advanced Task 2
    #
    # The instructions for the advanced task 2 of the group assignment are given in
    # the markdown file "assignment-advanced-2-tasks.md" at the root of the repository.
    # This file contains only the starting code not the instructions.
    #
    # A markdown file "assignment-example-outputs-advanced-2.md" at the root of the repository contains example outputs for the tasks.
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



    print_task_line(ADVANCED, 2)
    # Advanced Task 2 - Wikipedia articles

    tenMostFrequentWordsDF: DataFrame = __MISSING__IMPLEMENTATION__

    print("Top 10 most frequent words across all articles:")
    tenMostFrequentWordsDF.show(truncate=False)



    softwareArticles: list[str] = __MISSING__IMPLEMENTATION__

    print("Articles in alphabetical order where the word 'software' appears more than 5 times:")
    for title in softwareArticles:
        print(f" - {title}")



    longestWordsDF: DataFrame = __MISSING__IMPLEMENTATION__

    print("The longest words appearing in at least 10%, 25%, 50%, 75, and 90% of the articles:")
    longestWordsDF.show(truncate=False)



    frequentWordsDF: DataFrame = __MISSING__IMPLEMENTATION__

    print("Top 5 most frequent words per article (excluding forbidden words) in articles last updated before October 2025:")
    frequentWordsDF.show(truncate=False)
