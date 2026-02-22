"""The tasks for exercise 4."""

# Copyright 2025 Tampere University
# This notebook and software was developed for a Tampere University course COMP.CS.320.
# This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
# Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

# some imports that might be required in the tasks
import dataclasses
import re

from pyspark.rdd import RDD
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType


def main():
    # COMP.CS.320 Data-Intensive Programming, Exercise 4
    #
    # This exercise is in three parts.
    # - Tasks 1-4 are basic tasks of using RDDs with text based data.
    # - Tasks 5-7 are similar basic tasks for text data but using DataFrames instead.
    # - Task 8 is a theory question related to the lectures.
    #
    # This is the Python version intended for local development.
    #
    # Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    # There is test code and example output following most of the tasks that involve producing code.
    #
    # At the end of the file, there is a question regarding the use of AI or other collaboration when working the tasks.
    # Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle.


    # Some resources that can help with the tasks in this exercise:
    #
    # - The tutorial notebook from our course: in the repository at: /ex1/Basics-of-using-Databricks-notebooks.ipynb
    # - Chapters 3 and 6 in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
    #     - There are additional code examples in the related GitHub repository: https://github.com/databricks/LearningSparkV2
    #     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    #       https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc
    # - Apache Spark RDD Programming Guide: https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html
    # - Apache Spark Datasets and DataFrames documentation: https://spark.apache.org/docs/3.5.6/sql-programming-guide.html#datasets-and-dataframes
    # - Databricks tutorial of using Spark DataFrames: https://docs.databricks.com/en/getting-started/dataframes.html
    # - Apache Spark documentation on all available functions that can be used on DataFrames:
    #   https://spark.apache.org/docs/3.5.6/sql-ref-functions.html
    # The full Spark Python functions API listing for the functions package might have some additional functions listed that
    # have not been updated in the documentation: https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html

    # In Databricks, the Spark session is created automatically, and you should not create it yourself.
    spark: SparkSession = SparkSession \
        .builder \
        .appName("ex4") \
        .config("spark.driver.host", "localhost") \
        .master("local") \
        .getOrCreate()

    # suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("WARN")



    printTaskLine(1)
    # Task 1 - Loading text data into an RDD
    #
    # In the weekly-exercises repository, the "data/ex4/wiki" folder
    # contains texts from selected Wikipedia articles in raw text format.
    #
    # - Load all articles into a single RDD. Exclude all empty lines from the RDD.
    # - Count the total number of non-empty lines in the article collection.
    # - Pick the first 7 lines from the created RDD and print them out.

    wikitextsRDD: RDD[str] = __MISSING__IMPLEMENTATION__

    numberOfLines: int = __MISSING__IMPLEMENTATION__

    lines7: list[str] = __MISSING__IMPLEMENTATION__


    print(f"The number of lines with text: {numberOfLines}")
    print("===================================")
    # Print the first 100 characters of the first 7 lines
    print(*[line[:100] for line in lines7], sep="\n")


    # Example output:
    # ===============
    # (the first lines depend on which file is loaded first and can be different)
    #
    # The number of lines with text: 1113
    # ===================================
    # Software engineering
    # Software engineering is an engineering approach to software development. A practitioner, called a so
    # The terms programmer and coder overlap software engineer, but they imply only the construction aspec
    # A software engineer applies a software development process, which involves defining, implementing, t
    # History
    # Beginning in the 1960s, software engineering was recognized as a separate field of engineering.
    # The development of software engineering was seen as a struggle. Problems included software that was



    printTaskLine(2)
    # Task 2 - Counting the number of words
    #
    # Using the RDD from task 1 as a starting point:
    #
    # - Create `wordsRdd` where each row contains one word with the following rules for words:
    #     - The word must have at least one character.
    #     - The word must not contain any numbers, i.e. the digits 0-9.
    # - Calculate the total number of words in the article collection using `wordsRDD`.
    # - Calculate the total number of distinct words in the article collection using the same criteria for the words.
    #
    # You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).
    # In this exercise you can ignore capitalization, parenthesis, and punctuation characters.
    # I.e., `word`, `Word`, `WORD`, `word.`, `(word)`, and `word).` should all be considered as valid and distinct words for this exercise.

    # First create RDD[str] for the words, and then filter out the unwanted words
    wordsRDD: RDD[str] = __MISSING__IMPLEMENTATION__

    numberOfWords: float = __MISSING__IMPLEMENTATION__

    numberOfDistinctWords: float = __MISSING__IMPLEMENTATION__


    print(f"The total number of words not containing digits: {numberOfWords}")
    print(f"The total number of distinct words not containing digits: {numberOfDistinctWords}")


    # Example output:
    # ===============
    # The total number of words not containing digits: 44604
    # The total number of distinct words not containing digits: 9463



    printTaskLine(3)
    # Task 3 - Counting word occurrences with RDD
    #
    # - Using the article collection data, create a pair RDD, `wordCountRDD`,
    #   where each row contains a distinct word and the count for how many times the word can be found in the collection.
    #     - Use the same word criteria as in task 2, i.e., ignore words which contain digits.
    #
    # - Using the created pair RDD, find what is the most common 8-letter word that is not `software`
    #   and starts with an `s`, and what is its count in the collection.
    #     - You can modify the given code and find the word and its count separately if that seems easier for you.

    wordCountRDD: RDD[tuple[str, int]] = __MISSING__IMPLEMENTATION__

    askedWord, wordCount = __MISSING__IMPLEMENTATION__


    print(f"First row in wordCountRDD: word: '{wordCountRDD.first()[0]}', count: {wordCountRDD.first()[1]}")
    print(f"The most common 8-letter word that is not 'software' and starts with 's': '{askedWord}' (appears {wordCount} times)")


    # Example output:
    # ===============
    # (the values for the first row depend on the code, and can be different than what is shown here)
    #
    # First row in wordCountRDD: word: 'practitioner,', count: 1
    # The most common 8-letter word that is not 'software' and starts with 's': 'specific' (appears 41 times)



    printTaskLine(4)
    # Task 4 - Digit occurrences in text
    #
    # Using the article collection data, create a pair RDD, `digitsRDD`, where each row contains digits from 0 to 9,
    # and the average number of times each digit appears in a line in the source data.
    # Order the RDD such that the most common digit is given first.
    #
    # As before, ignore all those lines that are empty in the article collection in this task.

    digitsRDD: RDD[tuple[int, float]] = __MISSING__IMPLEMENTATION__


    print("The average number of times the digits appear in the source data lines:")
    for row in digitsRDD.collect():
        print(f"Digit '{row[0]}': => average: {row[1]}")


    # Example output:
    # ===============
    # The average number of times the digits appear in the source data lines:
    # Digit '0': => average: 0.33513027852650495
    # Digit '2': => average: 0.2857142857142857
    # Digit '1': => average: 0.2776280323450135
    # Digit '9': => average: 0.18418688230008984
    # Digit '8': => average: 0.07457322551662174
    # Digit '6': => average: 0.0673854447439353
    # Digit '3': => average: 0.0637915543575921
    # Digit '4': => average: 0.05840071877807727
    # Digit '7': => average: 0.05660377358490566
    # Digit '5': => average: 0.05390835579514825



    printTaskLine(5)
    # Task 5 - Handling text data with DataFrames
    #
    # In this task the same Wikipedia article collection as in the RDD tasks 1-4 is used.
    # In the weekly-exercises repository, the "data/ex4/wiki" folder
    # contains texts from selected Wikipedia articles in raw text format.
    #
    # Scala supports Datasets, which are typed DataFrames, and the corresponding Scala
    # task is about using them instead of RDDs to redo tasks 1 and 2.
    # The Datasets are not supported by pyspark, thus this Python task is about using DataFrames instead.
    #
    # Part 1:
    # - Do task 1 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
    #     - Load all articles into a single `DataFrame`. Exclude all empty lines from the DataFrame.
    #     - Count the total number of non-empty lines in the article collection.
    #     - Pick the first 7 lines from the created DataFrame and print them out.
    #
    # Part 2:
    # - Do task 2 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
    # - Using the DataFrame from first part of this task as a starting point:
    #     - Create `wordsDF` where each row contains one word with the following rules for words:
    #         - The word must have at least one character.
    #         - The word must not contain any numbers, i.e. the digits 0-9.
    #     - Calculate the total number of words in the article collection using `wordsDF`.
    #     - Calculate the total number of distinct words in the article collection using the same criteria for the words.
    #
    # You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).
    # In this exercise you can ignore capitalization, parenthesis, and punctuation characters.
    # I.e., `word`, `Word`, `word.`, and `(word)` should all be considered as valid and distinct words for this exercise.

    wikitextsDF: DataFrame = __MISSING__IMPLEMENTATION__

    linesInDF: int = __MISSING__IMPLEMENTATION__

    first7Lines: list[str] = __MISSING__IMPLEMENTATION__


    print(f"The number of lines with text: {linesInDF}")
    print("===================================")
    # Print the first 100 characters of the first 7 lines
    print(*[line[:100] for line in first7Lines], sep="\n")

    # Use show() to show the first 7 lines
    wikitextsDF.show(7)


    wordsDF: DataFrame = __MISSING__IMPLEMENTATION__

    wordsInDF: int = __MISSING__IMPLEMENTATION__

    distinctWordsInDF: int = __MISSING__IMPLEMENTATION__


    print(f"The total number of words not containing digits: {wordsInDF}")
    print(f"The total number of distinct words not containing digits: {distinctWordsInDF}")


    # Example output:
    # ===============
    # The number of lines with text: 1113
    # ===================================
    # Artificial intelligence
    # Artificial intelligence (AI), in its broadest sense, is intelligence exhibited by machines, particul
    # Some high-profile applications of AI include advanced web search engines (e.g., Google Search); reco
    # The various subfields of AI research are centered around particular goals and the use of particular
    # Artificial intelligence was founded as an academic discipline in 1956, and the field went through mu
    # Goals
    # The general problem of simulating (or creating) intelligence has been broken into subproblems. These
    # +--------------------+
    # |               value|
    # +--------------------+
    # |Artificial intell...|
    # |Artificial intell...|
    # |Some high-profile...|
    # |The various subfi...|
    # |Artificial intell...|
    # |               Goals|
    # |The general probl...|
    # +--------------------+
    # only showing top 7 rows
    #
    # and
    #
    # The total number of words not containing digits: 44604
    # The total number of distinct words not containing digits: 9463



    printTaskLine(6)
    # Task 6 - Counting word occurrences with DataFrame
    #
    # Do task 3 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
    # Also, this time you should use the given data class `WordCount` for the final result
    # instead of `(str, int)` tuple like in task 3 with the RDD.
    #
    # - Using the article collection data, create a `DataFrame`, `wordCountDF`, where each row contains
    #   a distinct word and the count for how many times the word can be found in the collection.
    #     - Use the same word criteria as in task 2, i.e., ignore words which contain digits.
    #     - Have the column names in the DataFrame match the data class `WordCount`, i.e., `word` and `count`.
    #
    # - Using the created DataFrame, find what is the most common 8-letter word that is not `software`
    #   and starts with an `s`, and what is its count in the collection.
    #     - The straightforward way is to determine the word and its count together.
    #       However, you can determine the word and its count separately if you want.
    #     - Give the final result as an instance of the data class.

    @dataclasses.dataclass(frozen=True)
    class WordCount:
        word: str
        count: int


    wordCountDF: DataFrame = __MISSING__IMPLEMENTATION__

    askedWordCount: WordCount = __MISSING__IMPLEMENTATION__


    print(f"First row in wordCountDF: word: '{wordCountDF.first()['word']}', count: {wordCountDF.first()['count']}")
    print(f"Type of askedWordCount: {type(askedWordCount)}")
    print(f"The most common 8-letter word that is not 'software' and starts with 's': '{askedWordCount.word}' (appears {askedWordCount.count} times)")


    # Example output:
    # ===============
    # (the values for the first row depend on the code, and can be different than what is shown here)
    #
    # First row in wordCountDF: word: 'By', count: 12
    # Type of askedWordCount: <class '__main__.WordCount'>
    # The most common 8-letter word that is not 'software' and starts with 's': 'specific' (appears 41 times)



    printTaskLine(7)
    # Task 7 - Digit occurrences in text with DataFrames
    #
    # Do task 4 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
    #
    # - Using the article collection data, create a `DataFrame`, `digitDF`,
    #   where each row contains digits from 0 to 9, and the average number of times each digit appears
    #   in a line in the source data. Order the DataFrame such that the most common digit is given first.
    # - Collect the data from `digitDF` to a list of data class `DigitAverage` instances.
    #
    # As before, ignore all those lines that are empty in the article collection in this task.

    @dataclasses.dataclass(frozen=True)
    class DigitAverage:
        digit: int
        average: float


    digitDF: DataFrame = __MISSING__IMPLEMENTATION__

    digitAverages: list[DigitAverage] = __MISSING__IMPLEMENTATION__


    print("The average number of times the digits appear in the source data lines:")
    for digitAverage in digitAverages:
        print(f"Digit '{digitAverage.digit}': => average: {digitAverage.average}")


    # Example output:
    # ===============
    # The average number of times the digits appear in the source data lines:
    # Digit '0': => average: 0.33513027852650495
    # Digit '2': => average: 0.2857142857142857
    # Digit '1': => average: 0.2776280323450135
    # Digit '9': => average: 0.18418688230008984
    # Digit '8': => average: 0.07457322551662174
    # Digit '6': => average: 0.0673854447439353
    # Digit '3': => average: 0.0637915543575921
    # Digit '4': => average: 0.05840071877807727
    # Digit '7': => average: 0.05660377358490566
    # Digit '5': => average: 0.05390835579514825



    printTaskLine(8)
    # Task 8 - Theory question
    #
    # Using your own words, answer the following questions:
    #
    # 1. DataFrames, RDDs, and Datasets all have operations that can be divided into transformations and actions.
    #     - How would you describe transformations? And how would you describe actions?
    #     - How do actions differ from transformations?
    #     - Give some examples on both types of operations.
    # 2. Lazy evaluation was mentioned in the lectures.
    #     - What does lazy evaluation mean? How does it differ compared to eager evaluation?
    #     - Why is understanding lazy evaluation important when using Spark?
    #
    # Extensive answers are not required here.
    # If your answers do not fit into one screen, you have likely written more than what was expected.

    # ???



    printAIQuestionTaskLine()
    # Use of AI and collaboration
    #
    # Using AI and collaborating with other students is allowed when doing the weekly exercises.
    # However, the AI use and collaboration should be documented.
    #
    # - Did you use AI tools while doing this exercise?
    #   - Did they help? And how did they help?
    # - Did you work with other students to complete the tasks?
    #   - Only extensive collaboration is expected to be reported. If you only got help
    #     for a couple of the tasks, you don't need to report it here.

    # ???



    # Typically, at the end you would stop the Spark session to free up resources.
    # DO NOT do this in Databricks! It will restart the entire cluster for all users.
    # (that is why it is commented out here too, getting to the end will stop the session automatically)
    # spark.stop()



# Helper function to separate the task outputs from each other
def printTaskLine(taskNumber: int) -> None:
    print(f"======\nTask {taskNumber}\n======")

def printAIQuestionTaskLine() -> None:
    print("======\nAI and collaboration\n======")



if __name__ == "__main__":
    main()
