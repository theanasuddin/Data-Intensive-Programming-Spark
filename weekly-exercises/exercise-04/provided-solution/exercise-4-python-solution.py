# Databricks notebook source
# MAGIC %md
# MAGIC Copyright 2025 Tampere University<br>
# MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
# MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
# MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

# COMMAND ----------

# MAGIC %md
# MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 4
# MAGIC
# MAGIC This exercise is in three parts.
# MAGIC
# MAGIC - Tasks 1-4 are basic tasks of using RDDs with text based data.
# MAGIC - Tasks 5-7 are similar basic tasks for text data but using DataFrames instead.
# MAGIC - Task 8 is a theory question related to the lectures.
# MAGIC
# MAGIC This is the **Python** version, switch to the Scala version if you want to do the tasks in Scala.
# MAGIC
# MAGIC Each task has its own cell(s) for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary.<br>
# MAGIC There are cells with test code and example output following most of the tasks that involve producing code.
# MAGIC
# MAGIC At the end of the notebook, there is a question regarding the use of AI or other collaboration when working the tasks.<br>
# MAGIC Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle: [Weekly Exercise #4](https://moodle.tuni.fi/mod/assign/view.php?id=3503819)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Some resources that can help with the tasks in this exercise:
# MAGIC
# MAGIC - The [tutorial notebook](https://adb-7895492183558578.18.azuredatabricks.net/editor/notebooks/743402606902162) from our course
# MAGIC - Chapters 3 and 6 in [Learning Spark, 2nd Edition](https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
# MAGIC     - There are additional code examples in the related [GitHub repository](https://github.com/databricks/LearningSparkV2).
# MAGIC     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL<br> `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
# MAGIC - Apache Spark [RDD Programming Guide](https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html)
# MAGIC - Apache Spark [Datasets and DataFrames](https://spark.apache.org/docs/3.5.6/sql-programming-guide.html#datasets-and-dataframes) documentation
# MAGIC - [Databricks tutorial](https://docs.databricks.com/en/getting-started/dataframes.html) of using Spark DataFrames
# MAGIC - [Apache Spark documentation](https://spark.apache.org/docs/3.5.6/sql-ref-functions.html) on all available functions that can be used on DataFrames.<br>
# MAGIC   The full [Spark Python functions API listing](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html) for the functions package might have some additional functions listed that have not been updated in the documentation.

# COMMAND ----------

# some imports that might be required in the tasks
import dataclasses
import re

from pyspark.rdd import RDD
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Loading text data into an RDD
# MAGIC
# MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) the folder `exercises/ex4/wiki` contains texts from selected Wikipedia articles in raw text format.
# MAGIC
# MAGIC - Load all articles into a single RDD. Exclude all empty lines from the RDD.
# MAGIC - Count the total number of non-empty lines in the article collection.
# MAGIC - Pick the first 7 lines from the created RDD and print them out.

# COMMAND ----------

wikiTextPath: str = "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki"
wikitextsRDD: RDD[str] = spark \
    .sparkContext \
    .textFile(wikiTextPath) \
    .filter(lambda line: len(line) > 0)

numberOfLines: int = wikitextsRDD.count()

lines7: list[str] = wikitextsRDD.take(7)

# COMMAND ----------

print(f"The number of lines with text: {numberOfLines}")
print("===================================")
# Print the first 100 characters of the first 7 lines
print(*[line[:100] for line in lines7], sep="\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC The number of lines with text: 1113
# MAGIC ===================================
# MAGIC Artificial intelligence
# MAGIC Artificial intelligence (AI), in its broadest sense, is intelligence exhibited by machines, particul
# MAGIC Some high-profile applications of AI include advanced web search engines (e.g., Google Search); reco
# MAGIC The various subfields of AI research are centered around particular goals and the use of particular
# MAGIC Artificial intelligence was founded as an academic discipline in 1956, and the field went through mu
# MAGIC Goals
# MAGIC The general problem of simulating (or creating) intelligence has been broken into subproblems. These
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Counting the number of words
# MAGIC
# MAGIC Using the RDD from task 1 as a starting point:
# MAGIC
# MAGIC - Create `wordsRdd` where each row contains one word with the following rules for words:
# MAGIC     - The word must have at least one character.
# MAGIC     - The word must not contain any numbers, i.e. the digits 0-9.
# MAGIC - Calculate the total number of words in the article collection using `wordsRDD`.
# MAGIC - Calculate the total number of distinct words in the article collection using the same criteria for the words.
# MAGIC
# MAGIC You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).<br>
# MAGIC In this exercise you can ignore capitalization, parenthesis, and punctuation characters. I.e., `word`, `Word`, `WORD`, `word.`, `(word)`, and `word).` should all be considered as valid and distinct words for this exercise.

# COMMAND ----------

# First create RDD[str] for the words, and then filter out the unwanted words
wordsRDD: RDD[str] = wikitextsRDD \
    .flatMap(lambda line: line.split(" ")) \
    .filter(lambda word: word != "") \
    .filter(lambda word: all([not letter.isdigit() for letter in word]))

# The words that contain numbers could also be filtered out with a regular expression, for example:
#     .filter(lambda word: re.match("(.)*[0-9]+(.)*", word) is None)

numberOfWords: float = wordsRDD.count()

numberOfDistinctWords: float = wordsRDD.distinct().count()

# COMMAND ----------

print(f"The total number of words not containing digits: {numberOfWords}")
print(f"The total number of distinct words not containing digits: {numberOfDistinctWords}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC The total number of words not containing digits: 44604
# MAGIC The total number of distinct words not containing digits: 9463
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - Counting word occurrences with RDD
# MAGIC
# MAGIC - Using the article collection data, create a pair RDD, `wordCountRDD`, where each row contains a distinct word and the count for how many times the word can be found in the collection.
# MAGIC     - Use the same word criteria as in task 2, i.e., ignore words which contain digits.
# MAGIC
# MAGIC - Using the created pair RDD, find what is the most common 8-letter word that is not `software` and starts with an `s`, and what is its count in the collection.
# MAGIC     - You can modify the given code and find the word and its count separately if that seems easier for you.

# COMMAND ----------

# First transform to RDD[(str, int)] where all the integers are ones, and then add the ones together for all distinct words
wordCountRDD: RDD[tuple[str, int]] = wordsRDD \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda count1, count2: count1 + count2)

# The same without using pair RDD function reduceByKey
wordCountRDD2: RDD[tuple[str, int]] = wordsRDD \
    .groupBy(lambda word: word) \
    .map(lambda wordTuple: (wordTuple[0], len(wordTuple[1])))


askedWord, wordCount = wordCountRDD \
    .filter(lambda wordCount: wordCount[0] != "software" and wordCount[0][0] == "s" and len(wordCount[0]) == 8) \
    .reduce(
        lambda wordCount1, wordCount2: (
            wordCount1 if wordCount1[1] > wordCount2[1]
            else wordCount2
        )
    )

# Sorting could be used instead of reduce (but it would likely be less efficient)
askedWord2, wordCount2 = wordCountRDD2 \
    .filter(lambda wordCount: wordCount[0] != "software" and wordCount[0][0] == "s" and len(wordCount[0]) == 8) \
    .sortBy(lambda wordCount: wordCount[1], ascending=False) \
    .first()

# COMMAND ----------

print(f"First row in wordCountRDD: word: '{wordCountRDD.first()[0]}', count: {wordCountRDD.first()[1]}")
print(f"First row in wordCountRDD2: word: '{wordCountRDD2.first()[0]}', count: {wordCountRDD2.first()[1]}")
print(f"The most common 8-letter word that is not 'software' and starts with 's': '{askedWord}' (appears {wordCount} times)")
print(f"The most common 8-letter word that is not 'software' and starts with 's': '{askedWord2}' (appears {wordCount2} times)")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC (the values for the first row depend on the code, and can be different than what is shown here)
# MAGIC
# MAGIC ```text
# MAGIC First row in wordCountRDD: word: 'sense,', count: 1
# MAGIC The most common 8-letter word that is not 'software' and starts with 's': 'specific' (appears 41 times)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Digit occurrences in text
# MAGIC
# MAGIC Using the article collection data, create a pair RDD, `digitsRDD`, where each row contains digits from 0 to 9, and the average number of times each digit appears in a line in the source data. Order the RDD such that the most common digit is given first.
# MAGIC
# MAGIC As before, ignore all those lines that are empty in the article collection in this task.

# COMMAND ----------

digitsRDD: RDD[tuple[int, float]] = wikitextsRDD \
    .flatMap(
        lambda line: [
            (
                digit,
                (
                    1,
                    line.count(str(digit))
                )
            )
            for digit in range(10)
        ]
    ) \
    .reduceByKey(lambda count1, count2: (count1[0] + count2[0], count1[1] + count2[1])) \
    .mapValues(lambda counts: counts[1] / counts[0]) \
    .sortBy(lambda digitAverages: -digitAverages[1])

# COMMAND ----------

# Alternative version:
# --------------------
# explicitly calculating the counts for each digit
# and then explicitly grouping and calculating the averages

def get_averages(digit_counts: list[tuple[int, int]]):
    counts = [count for _, count in digit_counts]
    return sum(counts) / len(counts)

digitsRDD2: RDD[tuple[int, float]] = wikitextsRDD \
    .flatMap(
        lambda line: [
            (0, line.count('0')),
            (1, line.count('1')),
            (2, line.count('2')),
            (3, line.count('3')),
            (4, line.count('4')),
            (5, line.count('5')),
            (6, line.count('6')),
            (7, line.count('7')),
            (8, line.count('8')),
            (9, line.count('9')),
        ]
    ) \
    .groupBy(lambda row: row[0]) \
    .mapValues(get_averages) \
    .sortBy(lambda digitAverages: -digitAverages[1])

# COMMAND ----------

# Another alternate version, utilizing the previously calculated number of lines
digitsRDD3: RDD[tuple[int, float]] = wikitextsRDD \
    .flatMap(lambda line: line) \
    .filter(lambda character: character.isdigit()) \
    .map(lambda character: int(character)) \
    .groupBy(lambda digit: digit) \
    .mapValues(lambda digitList: len(digitList) / numberOfLines) \
    .sortBy(lambda digitAverages: -digitAverages[1])

# COMMAND ----------

print("The average number of times the digits appear in the source data lines:")
for row in digitsRDD.collect():
    print(f"Digit '{row[0]}': => average: {row[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC The average number of times the digits appear in the source data lines:
# MAGIC Digit '0': => average: 0.33513027852650495
# MAGIC Digit '2': => average: 0.2857142857142857
# MAGIC Digit '1': => average: 0.2776280323450135
# MAGIC Digit '9': => average: 0.18418688230008984
# MAGIC Digit '8': => average: 0.07457322551662174
# MAGIC Digit '6': => average: 0.0673854447439353
# MAGIC Digit '3': => average: 0.0637915543575921
# MAGIC Digit '4': => average: 0.05840071877807727
# MAGIC Digit '7': => average: 0.05660377358490566
# MAGIC Digit '5': => average: 0.05390835579514825
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Handling text data with DataFrames
# MAGIC
# MAGIC In this task the same Wikipedia article collection as in the RDD tasks 1-4 is used.<br>
# MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) the folder `exercises/ex4/wiki` contains the texts from selected Wikipedia articles in raw text format.
# MAGIC
# MAGIC Scala supports Datasets, which are typed DataFrames, and the corresponding Scala task is about using them instead of RDDs to redo tasks 1 and 2.<br>
# MAGIC The Datasets are not supported by pyspark, thus this Python task is about using DataFrames instead.
# MAGIC
# MAGIC Part 1:
# MAGIC
# MAGIC - Do task 1 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
# MAGIC     - Load all articles into a single `DataFrame`. Exclude all empty lines from the DataFrame.
# MAGIC     - Count the total number of non-empty lines in the article collection.
# MAGIC     - Pick the first 7 lines from the created DataFrame and print them out.
# MAGIC
# MAGIC Part 2:
# MAGIC
# MAGIC - Do task 2 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
# MAGIC - Using the DataFrame from first part of this task as a starting point:
# MAGIC     - Create `wordsDF` where each row contains one word with the following rules for words:
# MAGIC         - The word must have at least one character.
# MAGIC         - The word must not contain any numbers, i.e. the digits 0-9.
# MAGIC     - Calculate the total number of words in the article collection using `wordsDF`.
# MAGIC     - Calculate the total number of distinct words in the article collection using the same criteria for the words.
# MAGIC
# MAGIC You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).<br>
# MAGIC In this exercise you can ignore capitalization, parenthesis, and punctuation characters. I.e., `word`, `Word`, `word.`, and `(word)` should all be considered as valid and distinct words for this exercise.

# COMMAND ----------

wikitextsDF: DataFrame = spark \
    .read \
    .text(wikiTextPath) \
    .filter(F.col("value") != "")

linesInDF: int = wikitextsDF.count()

first7Lines: list[str] = [
    row[0]
    for row in wikitextsDF.take(7)
]

# COMMAND ----------

print(f"The number of lines with text: {linesInDF}")
print("===================================")
# Print the first 100 characters of the first 7 lines
print(*[line[:100] for line in first7Lines], sep="\n")

# Use show() to show the first 7 lines
wikitextsDF.show(7)

# COMMAND ----------

# first split the lines into an array of words, and the separate each word into its own row
# for the filtering, remove first empty words, and then use regular expression to remove words with digits
wordsDF: DataFrame = wikitextsDF \
    .withColumn("value", F.split(F.col("value"), " ")) \
    .withColumn("value", F.explode(F.col("value"))) \
    .filter(F.length(F.col("value")) > 0) \
    .filter(~F.regexp(F.col("value"), F.lit("(.)*[0-9]+(.)*")))

wordsInDF: int = wordsDF.count()

distinctWordsInDF: int = wordsDF.distinct().count()

# COMMAND ----------

print(f"The total number of words not containing digits: {wordsInDF}")
print(f"The total number of distinct words not containing digits: {distinctWordsInDF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC The number of lines with text: 1113
# MAGIC ===================================
# MAGIC Artificial intelligence
# MAGIC Artificial intelligence (AI), in its broadest sense, is intelligence exhibited by machines, particul
# MAGIC Some high-profile applications of AI include advanced web search engines (e.g., Google Search); reco
# MAGIC The various subfields of AI research are centered around particular goals and the use of particular
# MAGIC Artificial intelligence was founded as an academic discipline in 1956, and the field went through mu
# MAGIC Goals
# MAGIC The general problem of simulating (or creating) intelligence has been broken into subproblems. These
# MAGIC +--------------------+
# MAGIC |               value|
# MAGIC +--------------------+
# MAGIC |Artificial intell...|
# MAGIC |Artificial intell...|
# MAGIC |Some high-profile...|
# MAGIC |The various subfi...|
# MAGIC |Artificial intell...|
# MAGIC |               Goals|
# MAGIC |The general probl...|
# MAGIC +--------------------+
# MAGIC only showing top 7 rows
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC The total number of words not containing digits: 44604
# MAGIC The total number of distinct words not containing digits: 9463
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Counting word occurrences with DataFrame
# MAGIC
# MAGIC Do task 3 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.<br>
# MAGIC Also, this time you should use the given data class `WordCount` for the final result instead of `(str, int)` tuple like in task 3 with the RDD.
# MAGIC
# MAGIC - Using the article collection data, create a `DataFrame`, `wordCountDF`, where each row contains a distinct word and the count for how many times the word can be found in the collection.
# MAGIC     - Use the same word criteria as in task 2, i.e., ignore words which contain digits.
# MAGIC     - Have the column names in the DataFrame match the data class `WordCount`, i.e., `word` and `count`.
# MAGIC
# MAGIC - Using the created DataFrame, find what is the most common 8-letter word that is not `software` and starts with an `s`, and what is its count in the collection.
# MAGIC     - The straightforward way is to determine the word and its count together. However, you can determine the word and its count separately if you want.
# MAGIC     - Give the final result as an instance of the data class.

# COMMAND ----------

@dataclasses.dataclass(frozen=True)
class WordCount:
    word: str
    count: int

# COMMAND ----------

wordCountDF: DataFrame = wordsDF \
    .groupBy(F.col("value")) \
    .count() \
    .select(
        F.col("value").alias("word"),
        F.col("count").cast(IntegerType()).alias("count")
    )

# using aggregation with max and max_by to avoid sorting
askedWordCountRow: Row = wordCountDF \
    .filter((F.col("word") != "software") & (F.col("word").startswith("s")) & (F.length(F.col("word")) == 8)) \
    .agg(
        F.max_by(F.col("word"), F.col("count")).alias("word"),
        F.max(F.col("count")).alias("count")
    ) \
    .first()
# convert the Row to WordCount
askedWordCount: WordCount = WordCount(*askedWordCountRow)

# COMMAND ----------

print(f"First row in wordCountDF: word: '{wordCountDF.first()['word']}', count: {wordCountDF.first()['count']}")
print(f"Type of askedWordCount: {type(askedWordCount)}")
print(f"The most common 8-letter word that is not 'software' and starts with 's': '{askedWordCount.word}' (appears {askedWordCount.count} times)")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC (the values for the first row depend on the code, and can be different than what is shown here)
# MAGIC
# MAGIC ```text
# MAGIC First row in wordCountDF: word: 'By', count: 12
# MAGIC Type of askedWordCount: <class '__main__.WordCount'>
# MAGIC The most common 8-letter word that is not 'software' and starts with 's': 'specific' (appears 41 times)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - Digit occurrences in text with DataFrames
# MAGIC
# MAGIC Do task 4 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.<br>
# MAGIC
# MAGIC - Using the article collection data, create a `DataFrame`, `digitDF`, where each row contains digits from 0 to 9, and the average number of times each digit appears in a line in the source data. Order the DataFrame such that the most common digit is given first.
# MAGIC - Collect the data from `digitDF` to a list of data class `DigitAverage` instances.
# MAGIC
# MAGIC As before, ignore all those lines that are empty in the article collection in this task.

# COMMAND ----------

@dataclasses.dataclass(frozen=True)
class DigitAverage:
    digit: int
    average: float

# COMMAND ----------

digitDF: DataFrame = wikitextsDF \
    .withColumn(
        "digitCounts",
        F.array(
            [
                F.struct(
                    F.lit(digit).alias("digit"),
                    F.regexp_count(F.col("value"), F.lit(str(digit))).alias("count")
                )
                for digit in range(10)
            ]
        )
    ) \
    .withColumn("digitCounts", F.explode(F.col("digitCounts"))) \
    .select(
        F.col("digitCounts.digit").alias("digit"),
        F.col("digitCounts.count").alias("count")
    ) \
    .groupBy("digit") \
    .agg(F.avg(F.col("count")).alias("average")) \
    .orderBy(F.col("average").desc())


digitAverages: list[DigitAverage] = [
    DigitAverage(*row)
    for row in digitDF.collect()
]

# COMMAND ----------

print("The average number of times the digits appear in the source data lines:")
for digitAverage in digitAverages:
    print(f"Digit '{digitAverage.digit}': => average: {digitAverage.average}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC The average number of times the digits appear in the source data lines:
# MAGIC Digit '0': => average: 0.33513027852650495
# MAGIC Digit '2': => average: 0.2857142857142857
# MAGIC Digit '1': => average: 0.2776280323450135
# MAGIC Digit '9': => average: 0.18418688230008984
# MAGIC Digit '8': => average: 0.07457322551662174
# MAGIC Digit '6': => average: 0.0673854447439353
# MAGIC Digit '3': => average: 0.0637915543575921
# MAGIC Digit '4': => average: 0.05840071877807727
# MAGIC Digit '7': => average: 0.05660377358490566
# MAGIC Digit '5': => average: 0.05390835579514825
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - Theory question
# MAGIC
# MAGIC Using your own words, answer the following questions:
# MAGIC
# MAGIC 1. DataFrames, RDDs, and Datasets all have operations that can be divided into transformations and actions.
# MAGIC    - How would you describe transformations? And how would you describe actions?
# MAGIC    - How do actions differ from transformations?
# MAGIC    - Give some examples on both types of operations.
# MAGIC 2. Lazy evaluation was mentioned in the lectures.
# MAGIC     - What does lazy evaluation mean? How does it differ compared to eager evaluation?
# MAGIC     - Why is understanding lazy evaluation important when using Spark?
# MAGIC
# MAGIC Extensive answers are not required here.<br>
# MAGIC If your answers do not fit into one screen, you have likely written more than what was expected.

# COMMAND ----------

# MAGIC %md
# MAGIC 1.
# MAGIC
# MAGIC - Transformation are operations that create a new DataFrame (or RDD or Dataset) from an existing DataFrame. The new DataFrame is not computed immediately, but instead a logical plan for the computation is stored.
# MAGIC - Actions are operations that force the computation of results from DataFrames. Through actions, values computed from DataFrames can be stored to variables, shown to the user on screen, or written to a storage. Unlike transformations, actions propagate immediate computation.
# MAGIC - Example transformations: `map`, `filter`, `select`, `groupBy`
# MAGIC     - reading data from a source to a DataFrame is also a transformation, however the existence of the source will be checked immediately
# MAGIC - Example actions: `collect`, `count`, `show`, `reduce`
# MAGIC
# MAGIC 2.
# MAGIC
# MAGIC - Lazy evaluation means that the computation is not done until required. Eager evaluation means that the computation is done immediately.
# MAGIC - All transformations in Spark are lazily evaluated, while all actions propagate eager evaluation. This applies regardless of the programming language used, Scala, Python, or some other language. Due to the lazy evaluation, errors might appear later in the process than expected.
# MAGIC     - For example, you can build a large and complex chain of transformations, and they might seem to compute very fast if you run them without actions (since nothing is actually calculated). However, when you tried to show or store the results, you then could get an error related to the first step in the transformation chain.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use of AI and collaboration
# MAGIC
# MAGIC Using AI and collaborating with other students is allowed when doing the weekly exercises.<br>
# MAGIC However, the AI use and collaboration should be documented.
# MAGIC
# MAGIC - Did you use AI tools while doing this exercise?
# MAGIC   - Did they help? And how did they help?
# MAGIC - Did you work with other students to complete the tasks?
# MAGIC   - Only extensive collaboration is expected to be reported. If you only got help for a couple of the tasks, you don't need to report it here.

# COMMAND ----------

# MAGIC %md
# MAGIC AI tool usage and other collaboration should be mentioned here.
