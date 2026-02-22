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

wikitextsRDD: RDD[str] = sc.textFile(
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Artifical_intelligence.txt,"
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Computer_science.txt,"
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Data_analysis.txt,"
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Data_engineering.txt,"
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Data_science.txt,"
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Database.txt,"
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Machine_learning.txt,"
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Software_engineering.txt"
).filter(lambda line: line.strip() != "")

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

wordsRDD: RDD[str] = wikitextsRDD.flatMap(lambda line: line.split()).filter(
    lambda word: len(word) > 0 and not any(char.isdigit() for char in word)
)

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

wordCountRDD: RDD[tuple[str, int]] = wordsRDD.map(lambda word: (word, 1)).reduceByKey(
    lambda a, b: a + b
)

askedWord, wordCount = (
    wordCountRDD.filter(
        lambda x: len(x[0]) == 8
        and x[0].lower().startswith("s")
        and x[0].lower() != "software"
    )
    .sortBy(lambda x: x[1], ascending=False)
    .first()
)

# COMMAND ----------

print(f"First row in wordCountRDD: word: '{wordCountRDD.first()[0]}', count: {wordCountRDD.first()[1]}")
print(f"The most common 8-letter word that is not 'software' and starts with 's': '{askedWord}' (appears {wordCount} times)")

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

totalLines = wikitextsRDD.count()

digitsRDD: RDD[tuple[int, float]] = (
    wikitextsRDD.flatMap(lambda line: [(int(c), 1) for c in line if c.isdigit()])
    .map(lambda x: (x[0], x[1]))
    .reduceByKey(lambda a, b: a + b)
    .mapValues(lambda count: count / totalLines)
    .sortBy(lambda x: x[1], ascending=False)
)

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

wikitextsDF: DataFrame = spark.read.text(
    [
        "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Artifical_intelligence.txt",
        "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Computer_science.txt",
        "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Data_analysis.txt",
        "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Data_engineering.txt",
        "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Data_science.txt",
        "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Database.txt",
        "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Machine_learning.txt",
        "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex4/wiki/Software_engineering.txt",
    ]
).filter("value != ''")

linesInDF: int = wikitextsDF.count()

first7Lines: list[str] = [row.value for row in wikitextsDF.limit(7).collect()]

# COMMAND ----------

print(f"The number of lines with text: {linesInDF}")
print("===================================")
# Print the first 100 characters of the first 7 lines
print(*[line[:100] for line in first7Lines], sep="\n")

# Use show() to show the first 7 lines
wikitextsDF.show(7)

# COMMAND ----------

wordsDF: DataFrame = wikitextsDF.selectExpr(
    "explode(split(value, ' ')) as word"
).filter("word != '' and not word rlike '[0-9]'")

wordsInDF: int = wordsDF.count()

distinctWordsInDF: int = wordsDF.select("word").distinct().count()

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

wordCountDF: DataFrame = (
    wordsDF.groupBy("word").count().withColumnRenamed("count", "count")
)

askedWordCount: WordCount = (
    wordCountDF.filter(
        (F.length("word") == 8)
        & (F.lower("word").startswith("s"))
        & (F.lower("word") != "software")
    )
    .orderBy("count", ascending=False)
    .first()
)
askedWordCount = WordCount(word=askedWordCount.word, count=askedWordCount["count"])

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
# MAGIC ## Task 7 - Digit occurrences in text with Datasets
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

digitDF: DataFrame = (
    wikitextsDF.selectExpr("explode(split(value, '')) as char")
    .filter("char rlike '[0-9]'")
    .groupBy("char")
    .count()
    .withColumnRenamed("count", "count")
    .withColumn("average", F.col("count") / wikitextsDF.count())
    .select(F.col("char").cast("int").alias("digit"), "average")
    .orderBy("average", ascending=False)
)

digitAverages: list[DigitAverage] = [
    DigitAverage(row.digit, row.average) for row in digitDF.collect()
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
# MAGIC - Transformations are a type of operation that creates a new dataset from an existing dataset, but they do not immediately compute the result. Transformations are lazily evaluated. On the other hand, actions are also operations, but they trigger computation and return a result immediately or write the data to a storage.
# MAGIC
# MAGIC - So we can say, transformations define what to do, and actions actually execute the operations.
# MAGIC
# MAGIC - Transformations example: `map()`, `filter()`, `select()`, `groupBy()`, etc. <br /> Actions example: `count()`, `collect()`, `show()`, `saveAsTextFile()`, etc.
# MAGIC
# MAGIC 2. 
# MAGIC - Lazy evaluation means that Spark does not compute the transformations immediately, as we have already understood; it executes everything only when we perform an action call. On the contrary, eager evaluation is like any other normal operation, let's say normal Python operations, which compute results immediately after each operation.
# MAGIC
# MAGIC - If we understand lazy evaluation and its benefits, then we can improve the performance of our solutions by optimizing the chosen execution plan. We can avoid unnecessary computations, which saves time and resources when working with real-life big data.

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
# MAGIC - I used AI tools here and there, but mostly I have used the official Apache Spark doc, checked the book 'Learning Spark', the RDD programming guide, gone through the provided tutorial from Databricks, and the provided resources to find out about different functions and operations available in Spark.
# MAGIC - No, I worked on it on my own, just me and my machine.
