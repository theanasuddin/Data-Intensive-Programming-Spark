// Databricks notebook source
// MAGIC %md
// MAGIC Copyright 2025 Tampere University<br>
// MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
// MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
// MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

// COMMAND ----------

// MAGIC %md
// MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 4
// MAGIC
// MAGIC This exercise is in three parts.
// MAGIC
// MAGIC - Tasks 1-4 are basic tasks of using RDDs with text based data.
// MAGIC - Tasks 5-7 are similar basic tasks for text data but using Datasets instead.
// MAGIC - Task 8 is a theory question related to the lectures.
// MAGIC
// MAGIC This is the **Scala** version, switch to the Python version if you want to do the tasks in Python.
// MAGIC
// MAGIC Each task has its own cell(s) for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary.<br>
// MAGIC There are cells with test code and example output following most of the tasks that involve producing code.
// MAGIC
// MAGIC At the end of the notebook, there is a question regarding the use of AI or other collaboration when working the tasks.<br>
// MAGIC Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle: [Weekly Exercise #4](https://moodle.tuni.fi/mod/assign/view.php?id=3503819)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Some resources that can help with the tasks in this exercise:
// MAGIC
// MAGIC - The [tutorial notebook](https://adb-7895492183558578.18.azuredatabricks.net/editor/notebooks/743402606902162) from our course
// MAGIC - Chapters 3 and 6 in [Learning Spark, 2nd Edition](https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
// MAGIC     - There are additional code examples in the related [GitHub repository](https://github.com/databricks/LearningSparkV2).
// MAGIC     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL<br> `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
// MAGIC - Apache Spark [RDD Programming Guide](https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html)
// MAGIC - Apache Spark [Datasets and DataFrames](https://spark.apache.org/docs/3.5.6/sql-programming-guide.html#datasets-and-dataframes) documentation
// MAGIC - [Apache Spark documentation](https://spark.apache.org/docs/3.5.6/sql-ref-functions.html) on all available functions that can be used on DataFrames.<br>
// MAGIC   The full [Spark Scala functions API listing](https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/sql/functions$.html) for the functions package might have some additional functions listed that have not been updated in the documentation.

// COMMAND ----------

// some imports that might be required in the tasks
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

// this might be needed with Datasets
import spark.implicits._

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 1 - Loading text data into an RDD
// MAGIC
// MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) the folder `exercises/ex4/wiki` contains texts from selected Wikipedia articles in raw text format.
// MAGIC
// MAGIC - Load all articles into a single RDD. Exclude all empty lines from the RDD.
// MAGIC - Count the total number of non-empty lines in the article collection.
// MAGIC - Pick the first 7 lines from the created RDD and print them out.

// COMMAND ----------

val wikitextsRDD: RDD[String] = ???

val numberOfLines: Long = ???

val lines7: Array[String] = ???

// COMMAND ----------

println(s"The number of lines with text: ${numberOfLines}")
println("===================================")
// Print the first 100 characters of the first 7 lines
lines7.foreach(line => println(line.substring(0, Math.min(100, line.length()))))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC The number of lines with text: 1113
// MAGIC ===================================
// MAGIC Artificial intelligence
// MAGIC Artificial intelligence (AI), in its broadest sense, is intelligence exhibited by machines, particul
// MAGIC Some high-profile applications of AI include advanced web search engines (e.g., Google Search); reco
// MAGIC The various subfields of AI research are centered around particular goals and the use of particular
// MAGIC Artificial intelligence was founded as an academic discipline in 1956, and the field went through mu
// MAGIC Goals
// MAGIC The general problem of simulating (or creating) intelligence has been broken into subproblems. These
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 2 - Counting the number of words
// MAGIC
// MAGIC Using the RDD from task 1 as a starting point:
// MAGIC
// MAGIC - Create `wordsRdd` where each row contains one word with the following rules for words:
// MAGIC     - The word must have at least one character.
// MAGIC     - The word must not contain any numbers, i.e. the digits 0-9.
// MAGIC - Calculate the total number of words in the article collection using `wordsRDD`.
// MAGIC - Calculate the total number of distinct words in the article collection using the same criteria for the words.
// MAGIC
// MAGIC You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).<br>
// MAGIC In this exercise you can ignore capitalization, parenthesis, and punctuation characters. I.e., `word`, `Word`, `WORD`, `word.`, `(word)`, and `word).` should all be considered as valid and distinct words for this exercise.

// COMMAND ----------

val wordsRDD: RDD[String] = ???

val numberOfWords: Long = ???

val numberOfDistinctWords: Long = ???

// COMMAND ----------

println(s"The total number of words not containing digits: ${numberOfWords}")
println(s"The total number of distinct words not containing digits: ${numberOfDistinctWords}")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC The total number of words not containing digits: 44604
// MAGIC The total number of distinct words not containing digits: 9463
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 3 - Counting word occurrences with RDD
// MAGIC
// MAGIC - Using the article collection data, create a pair RDD, `wordCountRDD`, where each row contains a distinct word and the count for how many times the word can be found in the collection.
// MAGIC     - Use the same word criteria as in task 2, i.e., ignore words which contain digits.
// MAGIC
// MAGIC - Using the created pair RDD, find what is the most common 8-letter word that is not `software` and starts with an `s`, and what is its count in the collection.
// MAGIC     - You can modify the given code and find the word and its count separately if that seems easier for you.

// COMMAND ----------

val wordCountRDD: RDD[(String, Int)] = ???

val (askedWord: String, wordCount: Int) = ???

// COMMAND ----------

println(s"First row in wordCountRDD: word: '${wordCountRDD.first()._1}', count: ${wordCountRDD.first()._2}")
println(s"The most common 8-letter word that is not 'software' and starts with 's': '${askedWord}' (appears ${wordCount} times)")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC (the values for the first row depend on the code, and can be different than what is shown here)
// MAGIC
// MAGIC ```text
// MAGIC First row in wordCountRDD: word: 'logic.', count: 3
// MAGIC The most common 8-letter word that is not 'software' and starts with 's': 'specific' (appears 41 times)
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 4 - Digit occurrences in text
// MAGIC
// MAGIC Using the article collection data, create a pair RDD, `digitsRDD`, where each row contains digits from 0 to 9, and the average number of times each digit appears in a line in the source data. Order the RDD such that the most common digit is given first.
// MAGIC
// MAGIC As before, ignore all those lines that are empty in the article collection in this task.

// COMMAND ----------

val digitsRDD: RDD[(Int, Double)] = ???

// COMMAND ----------

println("The average number of times the digits appear in the source data lines:")
digitsRDD.collect().foreach(row => println(s"Digit '${row._1}': => average: ${row._2}"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC The average number of times the digits appear in the source data lines:
// MAGIC Digit '0': => average: 0.33513027852650495
// MAGIC Digit '2': => average: 0.2857142857142857
// MAGIC Digit '1': => average: 0.2776280323450135
// MAGIC Digit '9': => average: 0.18418688230008984
// MAGIC Digit '8': => average: 0.07457322551662174
// MAGIC Digit '6': => average: 0.0673854447439353
// MAGIC Digit '3': => average: 0.0637915543575921
// MAGIC Digit '4': => average: 0.05840071877807727
// MAGIC Digit '7': => average: 0.05660377358490566
// MAGIC Digit '5': => average: 0.05390835579514825
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 5 - Handling text data with Datasets
// MAGIC
// MAGIC In this task the same Wikipedia article collection as in the RDD tasks 1-4 is used.<br>
// MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) the folder `exercises/ex4/wiki` contains the texts from selected Wikipedia articles in raw text format.
// MAGIC
// MAGIC In Scala `DataFrame` is an alias to `Dataset[Row]` where Row is a generic class that can contain any kind of row.<br>
// MAGIC Instead of the generic DataFrame, data can also be handled with Spark using typed Datasets.
// MAGIC
// MAGIC Part 1:
// MAGIC
// MAGIC - Do task 1 again, but this time using the higher level `Dataset` instead of the low level `RDD`.
// MAGIC     - Load all articles into a single `Dataset[String]`. Exclude all empty lines from the Dataset.
// MAGIC     - Count the total number of non-empty lines in the article collection.
// MAGIC     - Pick the first 7 lines from the created Dataset and print them out.
// MAGIC
// MAGIC Part 2:
// MAGIC
// MAGIC - Do task 2 again, but this time using the higher level `Dataset` instead of the low level `RDD`.
// MAGIC - Using the Dataset from first part of this task as a starting point:
// MAGIC     - Create `wordsDataset` where each row contains one word with the following rules for words:
// MAGIC         - The word must have at least one character.
// MAGIC         - The word must not contain any numbers, i.e. the digits 0-9.
// MAGIC     - Calculate the total number of words in the article collection using `wordsDataset`.
// MAGIC     - Calculate the total number of distinct words in the article collection using the same criteria for the words.
// MAGIC
// MAGIC You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).<br>
// MAGIC In this exercise you can ignore capitalization, parenthesis, and punctuation characters. I.e., `word`, `Word`, `word.`, and `(word)` should all be considered as valid and distinct words for this exercise.
// MAGIC
// MAGIC Hint: if you did the tasks 1 and 2, this task should be quite easy.

// COMMAND ----------

val wikitextsDataset: Dataset[String] = ???

val linesInDs: Long = ???

val first7Lines: Array[String] = ???

// COMMAND ----------

println(s"The number of lines with text: ${linesInDs}")
println("===================================")
// Print the first 100 characters of the first 7 lines
first7Lines.foreach(line => println(line.substring(0, Math.min(100, line.length()))))

// Use show() to show the first 7 lines
wikitextsDataset.show(7)

// COMMAND ----------

val wordsDataset: Dataset[String] = ???

val wordsInDataset: Long = ???

val distinctWordsInDataset: Long = ???

// COMMAND ----------

println(s"The total number of words not containing digits: ${wordsInDataset}")
println(s"The total number of distinct words not containing digits: ${distinctWordsInDataset}")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC The number of lines with text: 1113
// MAGIC ===================================
// MAGIC Artificial intelligence
// MAGIC Artificial intelligence (AI), in its broadest sense, is intelligence exhibited by machines, particul
// MAGIC Some high-profile applications of AI include advanced web search engines (e.g., Google Search); reco
// MAGIC The various subfields of AI research are centered around particular goals and the use of particular
// MAGIC Artificial intelligence was founded as an academic discipline in 1956, and the field went through mu
// MAGIC Goals
// MAGIC The general problem of simulating (or creating) intelligence has been broken into subproblems. These
// MAGIC +--------------------+
// MAGIC |               value|
// MAGIC +--------------------+
// MAGIC |Artificial intell...|
// MAGIC |Artificial intell...|
// MAGIC |Some high-profile...|
// MAGIC |The various subfi...|
// MAGIC |Artificial intell...|
// MAGIC |               Goals|
// MAGIC |The general probl...|
// MAGIC +--------------------+
// MAGIC only showing top 7 rows
// MAGIC ```
// MAGIC
// MAGIC and
// MAGIC
// MAGIC ```text
// MAGIC The total number of words not containing digits: 44604
// MAGIC The total number of distinct words not containing digits: 9463
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 6 - Counting word occurrences with Dataset
// MAGIC
// MAGIC Do task 3 again, but this time using the higher level `Dataset` instead of the low level `RDD`.<br>
// MAGIC Also, this time you should use the given case class `WordCount` in the resulting Dataset instead of `(String, Int)` tuple like in task 3 with the RDD.
// MAGIC
// MAGIC - Using the article collection data, create a `Dataset[WordCount]`, `wordCountDataset`, where each row contains a distinct word and the count for how many times the word can be found in the collection.
// MAGIC     - Use the same word criteria as in task 2, i.e., ignore words which contain digits.
// MAGIC
// MAGIC - Using the created Dataset, find what is the most common 8-letter word that is not `software` and starts with an `s`, and what is its count in the collection.
// MAGIC     - The straightforward way is to fully utilize the case class can determine the word and its count together. However, you can determine the word and its count separately if you want, but give the result as an instance of the case class.
// MAGIC
// MAGIC In this task there are multiple ways that you can achieve the asked results.<br>
// MAGIC You are free to choose whatever process you prefer, as long as the starting point is the `wordsDataset` from task 5, and the intermediate Dataset has the type `Dataset[WordCount]`.

// COMMAND ----------

case class WordCount(
    word: String,
    count: Int
)

// COMMAND ----------

val wordCountDataset: Dataset[WordCount] = ???

val askedWordCount: WordCount = ???

// COMMAND ----------

println(s"First row in wordCountDataset: word: '${wordCountDataset.first().word}', count: ${wordCountDataset.first().count}")
println(s"The most common 8-letter word that is not 'software' and starts with 's': '${askedWordCount.word}' (appears ${askedWordCount.count} times)")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC (the values for the first row depend on the code, and can be different than what is shown here)
// MAGIC
// MAGIC ```text
// MAGIC First row in wordCountDataset: word: '"'Software', count: 1
// MAGIC The most common 8-letter word that is not 'software' and starts with 's': 'specific' (appears 41 times)
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 7 - Digit occurrences in text with Datasets
// MAGIC
// MAGIC Do task 4 again, but this time using the higher level `Dataset` instead of the low level `RDD`.<br>
// MAGIC Also, this time you should use the given case class `DigitAverage` in the resulting Dataset instead of `(Int, Double)` tuple like in task 4 with the RDD.
// MAGIC
// MAGIC Using the article collection data, create a `Dataset[DigitAverage]`, `digitDataset`, where each row contains digits from 0 to 9, and the average number of times each digit appears in a line in the source data. Order the Dataset such that the most common digit is given first.
// MAGIC
// MAGIC As before, ignore all those lines that are empty in the article collection in this task.<br>
// MAGIC In this task there are multiple ways that you can achieve the asked results.<br>
// MAGIC You are free to choose whatever process you prefer, as long as the final Dataset has the type `Dataset[DigitAverage]`.

// COMMAND ----------

case class DigitAverage(
    digit: Int,
    average: Double
)

// COMMAND ----------

val digitDataset: Dataset[DigitAverage] = ???

// COMMAND ----------

println("The average number of times the digits appear in the source data lines:")
digitDataset.collect().foreach(digitAverage => println(s"Digit '${digitAverage.digit}': => average: ${digitAverage.average}"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC The average number of times the digits appear in the source data lines:
// MAGIC Digit '0': => average: 0.33513027852650495
// MAGIC Digit '2': => average: 0.2857142857142857
// MAGIC Digit '1': => average: 0.2776280323450135
// MAGIC Digit '9': => average: 0.18418688230008984
// MAGIC Digit '8': => average: 0.07457322551662174
// MAGIC Digit '6': => average: 0.0673854447439353
// MAGIC Digit '3': => average: 0.0637915543575921
// MAGIC Digit '4': => average: 0.05840071877807727
// MAGIC Digit '7': => average: 0.05660377358490566
// MAGIC Digit '5': => average: 0.05390835579514825
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 8 - Theory question
// MAGIC
// MAGIC Using your own words, answer the following questions:
// MAGIC
// MAGIC 1. DataFrames, RDDs, and Datasets all have operations that can be divided into transformations and actions.
// MAGIC    - How would you describe transformations? And how would you describe actions?
// MAGIC    - How do actions differ from transformations?
// MAGIC    - Give some examples on both types of operations.
// MAGIC 2. Lazy evaluation was mentioned in the lectures.
// MAGIC     - What does lazy evaluation mean? How does it differ compared to eager evaluation?
// MAGIC     - Why is understanding lazy evaluation important when using Spark?
// MAGIC
// MAGIC Extensive answers are not required here.<br>
// MAGIC If your answers do not fit into one screen, you have likely written more than what was expected.

// COMMAND ----------

// MAGIC %md
// MAGIC ???

// COMMAND ----------

// MAGIC %md
// MAGIC ## Use of AI and collaboration
// MAGIC
// MAGIC Using AI and collaborating with other students is allowed when doing the weekly exercises.<br>
// MAGIC However, the AI use and collaboration should be documented.
// MAGIC
// MAGIC - Did you use AI tools while doing this exercise?
// MAGIC   - Did they help? And how did they help?
// MAGIC - Did you work with other students to complete the tasks?
// MAGIC   - Only extensive collaboration is expected to be reported. If you only got help for a couple of the tasks, you don't need to report it here.

// COMMAND ----------

// MAGIC %md
// MAGIC ???
