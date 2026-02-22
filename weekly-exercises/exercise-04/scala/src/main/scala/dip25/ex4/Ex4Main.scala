// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

package dip25.ex4

// some imports that might be required in the tasks
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}


// for task 6
case class WordCount(
    word: String,
    count: Int
)

// for task 7
case class DigitAverage(
    digit: Int,
    average: Double
)


object Ex4Main extends App {
    // COMP.CS.320 Data-Intensive Programming, Exercise 4
    //
    // This exercise is in three parts.
    // - Tasks 1-4 are basic tasks of using RDDs with text based data.
    // - Tasks 5-7 are similar basic tasks for text data but using Datasets instead.
    // - Task 8 is a theory question related to the lectures.
    //
    // This is the Scala version intended for local development.
    //
    // Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    // There is test code and example output following most of the tasks that involve producing code.
    //
    // At the end of the file, there is a question regarding the use of AI or other collaboration when working the tasks.
    // Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle.


    // Some resources that can help with the tasks in this exercise:
    //
    // - The tutorial notebook from our course: in the repository at: /ex1/Basics-of-using-Databricks-notebooks.ipynb
    // - Chapters 3 and 5 in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
    //     - There are additional code examples in the related GitHub repository: https://github.com/databricks/LearningSparkV2
    //     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    //       https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc
    // - Apache Spark RDD Programming Guide: https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html
    // - Apache Spark Datasets and DataFrames documentation: https://spark.apache.org/docs/3.5.6/sql-programming-guide.html#datasets-and-dataframes
    // - Apache Spark documentation on all available functions that can be used on DataFrames:
    //   https://spark.apache.org/docs/3.5.6/sql-ref-functions.html
    // The full Spark Scala functions API listing for the functions package might have some additional functions listed that
    // have not been updated in the documentation: https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/sql/functions$.html


    // In Databricks, the Spark session is created automatically, and you should not create it yourself.
	val spark: SparkSession = SparkSession
        .builder()
        .appName("ex4")
        .config("spark.driver.host", "localhost")
        .master("local")
        .getOrCreate()

    // suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())

    // this might be needed with Datasets
    import spark.implicits._



    printTaskLine(1)
    // Task 1 - Loading text data into an RDD
    //
    // In the weekly-exercises repository, the "data/ex4/wiki" folder
    // contains texts from selected Wikipedia articles in raw text format.
    //
    // - Load all articles into a single RDD. Exclude all empty lines from the RDD.
    // - Count the total number of non-empty lines in the article collection.
    // - Pick the first 7 lines from the created RDD and print them out.

    val wikitextsRDD: RDD[String] = ???

    val numberOfLines: Long = ???

    val lines7: Array[String] = ???


    println(s"The number of lines with text: ${numberOfLines}")
    println("===================================")
    // Print the first 100 characters of the first 7 lines
    lines7.foreach(line => println(line.substring(0, Math.min(100, line.length()))))


    // Example output:
    // ===============
    // (the first lines depend on which file is loaded first and can be different)
    //
    // The number of lines with text: 1113
    // ===================================
    // Software engineering
    // Software engineering is an engineering approach to software development. A practitioner, called a so
    // The terms programmer and coder overlap software engineer, but they imply only the construction aspec
    // A software engineer applies a software development process, which involves defining, implementing, t
    // History
    // Beginning in the 1960s, software engineering was recognized as a separate field of engineering.
    // The development of software engineering was seen as a struggle. Problems included software that was



    printTaskLine(2)
    // Task 2 - Counting the number of words
    //
    // Using the RDD from task 1 as a starting point:
    //
    // - Create `wordsRdd` where each row contains one word with the following rules for words:
    //     - The word must have at least one character.
    //     - The word must not contain any numbers, i.e. the digits 0-9.
    // - Calculate the total number of words in the article collection using `wordsRDD`.
    // - Calculate the total number of distinct words in the article collection using the same criteria for the words.
    //
    // You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).
    // In this exercise you can ignore capitalization, parenthesis, and punctuation characters.
    // I.e., `word`, `Word`, `WORD`, `word.`, `(word)`, and `word).` should all be considered as valid and distinct words for this exercise.

    val wordsRDD: RDD[String] = ???

    val numberOfWords: Long = ???

    val numberOfDistinctWords: Long = ???


    println(s"The total number of words not containing digits: ${numberOfWords}")
    println(s"The total number of distinct words not containing digits: ${numberOfDistinctWords}")


    // Example output:
    // ===============
    // The total number of words not containing digits: 44604
    // The total number of distinct words not containing digits: 9463



    printTaskLine(3)
    // Task 3 - Counting word occurrences with RDD
    //
    // - Using the article collection data, create a pair RDD, `wordCountRDD`,
    //   where each row contains a distinct word and the count for how many times the word can be found in the collection.
    //     - Use the same word criteria as in task 2, i.e., ignore words which contain digits.
    //
    // - Using the created pair RDD, find what is the most common 8-letter word that is not `software`
    //   and starts with an `s`, and what is its count in the collection.
    //     - You can modify the given code and find the word and its count separately if that seems easier for you.

    val wordCountRDD: RDD[(String, Int)] = ???

    val (askedWord: String, wordCount: Int) = ("???", 0)  // replace with proper code


    println(s"First row in wordCountRDD: word: '${wordCountRDD.first()._1}', count: ${wordCountRDD.first()._2}")
    println(s"The most common 8-letter word that is not 'software' and starts with 's': '${askedWord}' (appears ${wordCount} times)")


    // Example output:
    // ===============
    // (the values for the first row depend on the code, and can be different than what is shown here)
    //
    // First row in wordCountRDD: word: 'meaning),', count: 1
    // The most common 8-letter word that is not 'software' and starts with 's': 'specific' (appears 41 times)



    printTaskLine(4)
    // Task 4 - Digit occurrences in text
    //
    // Using the article collection data, create a pair RDD, `digitsRDD`, where each row contains digits from 0 to 9,
    // and the average number of times each digit appears in a line in the source data.
    // Order the RDD such that the most common digit is given first.
    //
    // As before, ignore all those lines that are empty in the article collection in this task.

    val digitsRDD: RDD[(Int, Double)] = ???


    println("The average number of times the digits appear in the source data lines:")
    digitsRDD.collect().foreach(row => println(s"Digit '${row._1}': => average: ${row._2}"))


    // Example output:
    // ===============
    // The average number of times the digits appear in the source data lines:
    // Digit '0': => average: 0.33513027852650495
    // Digit '2': => average: 0.2857142857142857
    // Digit '1': => average: 0.2776280323450135
    // Digit '9': => average: 0.18418688230008984
    // Digit '8': => average: 0.07457322551662174
    // Digit '6': => average: 0.0673854447439353
    // Digit '3': => average: 0.0637915543575921
    // Digit '4': => average: 0.05840071877807727
    // Digit '7': => average: 0.05660377358490566
    // Digit '5': => average: 0.05390835579514825



    printTaskLine(5)
    // Task 5 - Handling text data with Datasets
    //
    // In this task the same Wikipedia article collection as in the RDD tasks 1-4 is used.
    // In the weekly-exercises repository, the "data/ex4/wiki" folder
    // contains texts from selected Wikipedia articles in raw text format.
    //
    // In Scala `DataFrame` is an alias to `Dataset[Row]` where Row is a generic class that can contain any kind of row.
    // Instead of the generic DataFrame, data can also be handled with Spark using typed Datasets.
    //
    // Part 1:
    //
    // - Do task 1 again, but this time using the higher level `Dataset` instead of the low level `RDD`.
    //     - Load all articles into a single `Dataset[String]`. Exclude all empty lines from the Dataset.
    //     - Count the total number of non-empty lines in the article collection.
    //     - Pick the first 7 lines from the created Dataset and print them out.
    //
    // Part 2:
    //
    // - Do task 2 again, but this time using the higher level `Dataset` instead of the low level `RDD`.
    // - Using the Dataset from first part of this task as a starting point:
    //     - Create `wordsDataset` where each row contains one word with the following rules for words:
    //         - The word must have at least one character.
    //         - The word must not contain any numbers, i.e. the digits 0-9.
    //     - Calculate the total number of words in the article collection using `wordsDataset`.
    //     - Calculate the total number of distinct words in the article collection using the same criteria for the words.
    //
    // You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).
    // In this exercise you can ignore capitalization, parenthesis, and punctuation characters.
    // I.e., `word`, `Word`, `word.`, and `(word)` should all be considered as valid and distinct words for this exercise.
    //
    // Hint: if you did the tasks 1 and 2, this task should be quite easy.

    val wikitextsDataset: Dataset[String] = ???

    val linesInDs: Long = ???

    val first7Lines: Array[String] = ???


    println(s"The number of lines with text: ${linesInDs}")
    println("===================================")
    // Print the first 100 characters of the first 7 lines
    first7Lines.foreach(line => println(line.substring(0, Math.min(100, line.length()))))

    // Use show() to show the first 7 lines
    wikitextsDataset.show(7)


    val wordsDataset: Dataset[String] = ???

    val wordsInDataset: Long = ???

    val distinctWordsInDataset: Long = ???


    println(s"The total number of words not containing digits: ${wordsInDataset}")
    println(s"The total number of distinct words not containing digits: ${distinctWordsInDataset}")


    // Example output:
    // ===============
    // The number of lines with text: 1113
    // ===================================
    // Artificial intelligence
    // Artificial intelligence (AI), in its broadest sense, is intelligence exhibited by machines, particul
    // Some high-profile applications of AI include advanced web search engines (e.g., Google Search); reco
    // The various subfields of AI research are centered around particular goals and the use of particular
    // Artificial intelligence was founded as an academic discipline in 1956, and the field went through mu
    // Goals
    // The general problem of simulating (or creating) intelligence has been broken into subproblems. These
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Artificial intell...|
    // |Artificial intell...|
    // |Some high-profile...|
    // |The various subfi...|
    // |Artificial intell...|
    // |               Goals|
    // |The general probl...|
    // +--------------------+
    // only showing top 7 rows
    //
    // and
    //
    // The total number of words not containing digits: 44604
    // The total number of distinct words not containing digits: 9463



    printTaskLine(6)
    // Task 6 - Counting word occurrences with Dataset
    //
    // Do task 3 again, but this time using the higher level `Dataset` instead of the low level `RDD`.
    // Also, this time you should use the given case class `WordCount` in the resulting Dataset
    // instead of `(String, Int)` tuple like in task 3 with the RDD.
    // (the case class is defined at the top of this file, before the main object)
    //
    // - Using the article collection data, create a `Dataset[WordCount]`, `wordCountDataset`,
    //   where each row contains a distinct word and the count for how many times the word can be found in the collection.
    //     - Use the same word criteria as in task 2, i.e., ignore words which contain digits.
    //
    // - Using the created Dataset, find what is the most common 8-letter word that is not `software`
    //   and starts with an `s`, and what is its count in the collection.
    //     - The straightforward way is to fully utilize the case class can determine the word and its count together.
    //       However, you can determine the word and its count separately if you want, but give the result as an instance of the case class.
    //
    // In this task there are multiple ways that you can achieve the asked results.
    // You are free to choose whatever process you prefer, as long as the starting point is the `wordsDataset` from task 5,
    // and the intermediate Dataset has the type `Dataset[WordCount]`.

    val wordCountDataset: Dataset[WordCount] = ???

    val askedWordCount: WordCount = ???


    println(s"First row in wordCountDataset: word: '${wordCountDataset.first().word}', count: ${wordCountDataset.first().count}")
    println(s"The most common 8-letter word that is not 'software' and starts with 's': '${askedWordCount.word}' (appears ${askedWordCount.count} times)")


    // Example output:
    // ===============
    // (the values for the first row depend on the code, and can be different than what is shown here)
    //
    // First row in wordCountDataset: word: '"'Software', count: 1
    // The most common 8-letter word that is not 'software' and starts with 's': 'specific' (appears 41 times)



    printTaskLine(7)
    // Task 7 - Digit occurrences in text with Datasets
    //
    // Do task 4 again, but this time using the higher level `Dataset` instead of the low level `RDD`.
    // Also, this time you should use the given case class `DigitAverage` in the resulting Dataset
    // instead of `(Int, Double)` tuple like in task 4 with the RDD.
    //
    // Using the article collection data, create a `Dataset[DigitAverage]`, `digitDataset`,
    // where each row contains digits from 0 to 9, and the average number of times each digit appears
    // in a line in the source data. Order the Dataset such that the most common digit is given first.
    //
    // As before, ignore all those lines that are empty in the article collection in this task.
    // In this task there are multiple ways that you can achieve the asked results.
    // You are free to choose whatever process you prefer, as long as the final Dataset has the type `Dataset[DigitAverage]`.

    val digitDataset: Dataset[DigitAverage] = ???


    println("The average number of times the digits appear in the source data lines:")
    digitDataset.collect().foreach(digitAverage => println(s"Digit '${digitAverage.digit}': => average: ${digitAverage.average}"))


    // Example output:
    // ===============
    // The average number of times the digits appear in the source data lines:
    // Digit '0': => average: 0.33513027852650495
    // Digit '2': => average: 0.2857142857142857
    // Digit '1': => average: 0.2776280323450135
    // Digit '9': => average: 0.18418688230008984
    // Digit '8': => average: 0.07457322551662174
    // Digit '6': => average: 0.0673854447439353
    // Digit '3': => average: 0.0637915543575921
    // Digit '4': => average: 0.05840071877807727
    // Digit '7': => average: 0.05660377358490566
    // Digit '5': => average: 0.05390835579514825



    printTaskLine(8)
    // Task 8 - Theory question
    //
    // Using your own words, answer the following questions:
    //
    // 1. DataFrames, RDDs, and Datasets all have operations that can be divided into transformations and actions.
    //     - How would you describe transformations? And how would you describe actions?
    //     - How do actions differ from transformations?
    //     - Give some examples on both types of operations.
    // 2. Lazy evaluation was mentioned in the lectures.
    //     - What does lazy evaluation mean? How does it differ compared to eager evaluation?
    //     - Why is understanding lazy evaluation important when using Spark?
    //
    // Extensive answers are not required here.
    // If your answers do not fit into one screen, you have likely written more than what was expected.

    // ???



    printAIQuestionTaskLine()
    // Use of AI and collaboration
    //
    // Using AI and collaborating with other students is allowed when doing the weekly exercises.
    // However, the AI use and collaboration should be documented.
    //
    // - Did you use AI tools while doing this exercise?
    //   - Did they help? And how did they help?
    // - Did you work with other students to complete the tasks?
    //   - Only extensive collaboration is expected to be reported. If you only got help
    //     for a couple of the tasks, you don't need to report it here.

    // ???



    // Helper function to separate the task outputs from each other
    def printTaskLine(taskNumber: Int): Unit = {
        println(s"======\nTask $taskNumber\n======")
    }

    def printAIQuestionTaskLine(): Unit = {
        println("======\nAI and collaboration\n======")
    }

    // Typically, at the end you would stop the Spark session to free up resources.
    // DO NOT do this in Databricks! It will restart the entire cluster for all users.
    // (that is why it is commented out here too, getting to the end will stop the session automatically)
    // spark.stop()
}
