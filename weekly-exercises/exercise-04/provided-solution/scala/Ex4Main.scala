// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

// The example solutions for exercise 4
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
        .appName("ex4-solution")
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

    val wikiTextPath: String = "../../data/ex4/wiki"
    val wikitextsRDD: RDD[String] = spark
        .sparkContext
        .textFile(wikiTextPath)
        .filter(line => line.nonEmpty)

    val numberOfLines: Long = wikitextsRDD.count()

    val lines7: Array[String] = wikitextsRDD.take(7)


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

    val wordsRDD: RDD[String] = wikitextsRDD
        .flatMap(line => line.split(" "))                        // RDD[String] for the words
        .filter(word => word != "")                              // remove empty words
        .filter(word => !word.exists(letter => letter.isDigit))  // remove words that contain a digit

    // The words that contain numbers could also be filtered out with a regular expression, for example:
    //     .filter(word => !word.matches("(.)*[0-9]+(.)*"))  // remove words that contain a digit

    val numberOfWords: Long = wordsRDD.count()

    val numberOfDistinctWords: Long = wordsRDD.distinct().count()


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

    val wordCountRDD: RDD[(String, Int)] = wordsRDD
        .map(word => (word, 1))                            // RDD[(String, Int)] where all the integers are ones
        .reduceByKey((count1, count2) => count1 + count2)  // add the ones together for all distinct words

    // The same without using pair RDD function reduceByKey
    val wordCountRDD2: RDD[(String, Int)] = wordsRDD            // RDD[String]
        .groupBy(word => word)                                  // RDD[(String, Iterable[String])]
        .map({case (word, wordList) => (word, wordList.size)})  // RDD[(String, Int)]


    val (askedWord: String, wordCount: Int) = wordCountRDD
        .filter({case (word, count) => word != "software" && word.startsWith("s") && word.length() == 8})
        .reduce({
            case ((word1, count1), (word2, count2)) => {
                if (count1 > count2) (word1, count1)
                else (word2, count2)
            }
        })

    // Sorting could be used instead of reduce (but it would likely be less efficient)
    val (askedWord2: String, wordCount2: Int) = wordCountRDD2
        .filter({case (word, count) =>  word != "software" && word.startsWith("s") && word.length() == 8})
        .sortBy({case (word, count) => count}, ascending = false)
        .first()


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

    val digitsRDD: RDD[(Int, Double)] = wikitextsRDD
        .flatMap(
            line => 0.to(9).map(
                digit => (
                    digit,
                    (
                        1,
                        line.count(character => character == '0' + digit.toChar)
                    )
                )
            )
        )
        .reduceByKey((count1, count2) => (count1._1 + count2._1, count1._2 + count2._2))
        .mapValues({case (lineCount, digitCount) => digitCount.toDouble / lineCount.toDouble})
        .sortBy({case (_, digitAvg) => -digitAvg})

    // Alternative version:
    // -------------------
    // explicitly calculating the counts for each digit
    // and then explicitly grouping and calculating the averages
    val digitsRDD2: RDD[(Int, Double)] = wikitextsRDD
        .flatMap(
            line => Seq(
                (0, line.count(character => character == '0')),
                (1, line.count(character => character == '1')),
                (2, line.count(character => character == '2')),
                (3, line.count(character => character == '3')),
                (4, line.count(character => character == '4')),
                (5, line.count(character => character == '5')),
                (6, line.count(character => character == '6')),
                (7, line.count(character => character == '7')),
                (8, line.count(character => character == '8')),
                (9, line.count(character => character == '9')),
            )
        )  // RDD[(Int, Int)]
        .groupBy(_._1)  // group by the digit
        .mapValues(
            digitCounts => {
                val counts = digitCounts.map(_._2)
                counts.sum.toDouble / counts.size
            }
        )
        .sortBy(-_._2)  // sort by the averages in descending order


    // Another alternate version, utilizing the previously calculated number of lines
    val digitsRDD3: RDD[(Int, Double)] = wikitextsRDD
        .flatMap(line => line.filter(character => character.isDigit))
        .map(character => (character-'0').toInt)
        .groupBy(digit => digit)
        .mapValues(digitList => digitList.size.toDouble / numberOfLines)
        .sortBy(-_._2)


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

    val wikitextsDataset: Dataset[String] = spark
        .read
        .text(wikiTextPath)  // using path defined in task 1
        .as[String]  // transforms the Dataset[Row] into Dataset[String]
        .filter(line => line.nonEmpty)

    val linesInDs: Long = wikitextsDataset.count()

    val first7Lines: Array[String] = wikitextsDataset.take(7)


    println(s"The number of lines with text: ${linesInDs}")
    println("===================================")
    // Print the first 100 characters of the first 7 lines
    first7Lines.foreach(line => println(line.substring(0, Math.min(100, line.length()))))

    // Use show() to show the first 7 lines
    wikitextsDataset.show(7)


    val wordsDataset: Dataset[String] = wikitextsDataset
        .flatMap(line => line.split(" "))                        // Dataset[String] for the words
        .filter(word => word != "")                              // remove empty words
        .filter(word => !word.exists(letter => letter.isDigit))  // remove words that contain a digit

    // The words that contain numbers could also be filtered out with a regular expression, for example:
        // .filter(word => !word.matches("(.)*[0-9]+(.)*"))  // remove words that contain a digit


    val wordsInDataset: Long = wordsDataset.count()

    val distinctWordsInDataset: Long = wordsDataset.distinct().count()


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

    val wordCountDataset: Dataset[WordCount] = wordsDataset
        .groupByKey((word: String) => word)
        .mapGroups((word: String, wordList: Iterator[String]) => WordCount(word, wordList.toSeq.size))


    // using reduce to make only one pass to find the most common remaining word
    val askedWordCount: WordCount = wordCountDataset
        .filter((wordCount: WordCount) => wordCount.word != "software" && wordCount.word.startsWith("s") && wordCount.word.length() == 8)
        .reduce(
            (wordCount1: WordCount, wordCount2: WordCount) => {
                if (wordCount1.count > wordCount2.count) wordCount1
                else wordCount2
            }
        )


    // the same using tuples instead and casting to case class only at the end
    // similar to the RDD code from task 3 except for the extensive use of explicit types (which are mostly optional)

    val wordCountDataset2: Dataset[WordCount] = wordsDataset
        .groupByKey((word: String) => word)
        .mapGroups((word: String, wordList: Iterator[String]) => (word, wordList.toSeq.size))
        .map({case (word: String, count: Int) => WordCount(word, count)})

    val (askedWordTemp: String, wordCountTemp: Int) = wordCountDataset2
        .map((wordCount: WordCount) => (wordCount.word, wordCount.count))
        .filter({case (word: String, count: Int) => word != "software" && word.startsWith("s") && word.length() == 8}: ((String, Int)) => Boolean)
        .reduce({
            case ((word1: String, count1: Int), (word2: String, count2: Int)) => {
                if (count1 > count2) (word1, count1)
                else (word2, count2)
            }
        }: ((String, Int), (String, Int)) => (String, Int))
    val askedWordCount2: WordCount =  WordCount(askedWordTemp, wordCountTemp)


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

    val digitDataset: Dataset[DigitAverage] = wikitextsDataset
        .flatMap(
            line => 0.to(9).map(
                digit => (
                    digit,
                    (
                        1,
                        line.count(character => character == '0' + digit.toChar)
                    )
                )
            )
        )  // Dataset[(Int, (Int, Int))]
        .groupByKey(_._1)  // group by the first column, i.e., the digit
        .mapGroups({
            case (digit, iter) => {
                val (totalCount, totalOccurrences) = iter.map(_._2)  // iterate by line and digit counts
                    .foldLeft((0, 0))({
                        case ((lineCount1, digitCount1), (lineCount2, digitCount2)) =>
                            (lineCount1 + lineCount2, digitCount1 + digitCount2)
                        })
                DigitAverage(digit, totalOccurrences.toDouble / totalCount.toDouble)
            }
        })
        .orderBy(col("average").desc)  // sorting requires the use of the column names with Datasets


    println("The average number of times the digits appear in the source data lines:")
    digitDataset.collect().foreach(digitAverage => println(s"Digit '${digitAverage.digit}': => average: ${digitAverage.average}"))


    // another alternative for tasks 5-7 using mostly DataFrame approach casting to typed Dataset only at the points required by the given tasks

    val wikitextsDataset3: Dataset[String] = spark
        .read
        .text(wikiTextPath)
        .filter(col("value") =!= "")  // for text data source the default column name is "value"
        .as[String]

    val wordsDataset3: Dataset[String] = wikitextsDataset3
        .withColumn("value", split(col("value"), " "))  // split the lines into an array of words
        .withColumn("value", explode(col("value")))     // separate each word into its own row
        .filter(len(col("value")) > 0)
        .filter(!regexp(col("value"), lit("(.)*[0-9]+(.)*")))
        .as[String]

    val wordCountDataset3: Dataset[WordCount] = wordsDataset3
        .groupBy(col("value"))
        .count()
        .select(col("value").as("word"), col("count").cast(IntegerType).as("count"))
        .as[WordCount]

    // using aggregation with max and max_by to avoid sorting
    val askedWordCount3: WordCount = wordCountDataset3
        .filter(col("word") =!= "software" && col("word").startsWith("s") && len(col("word")) === 8)
        .agg(
            max_by(col("word"), col("count")).as("word"),
            max(col("count")).as("count")
        )
        .as[WordCount]
        .first()

    val digitDataset3: Dataset[DigitAverage] = wikitextsDataset3
        .withColumn(
            "digitCounts",
            array(
                0.to(9).map(
                    digit => struct(
                        lit(digit).as("digit"),
                        regexp_count(col("value"), lit(s"${digit}")).as("count")
                    )
                ):_*
            )
        )
        .withColumn("digitCounts", explode(col("digitCounts")))
        .select(
            col("digitCounts.digit").as("digit"),
            col("digitCounts.count").as("count")
        )
        .groupBy("digit")
        .agg(avg(col("count")).as("average"))
        .orderBy(col("average").desc)
        .as[DigitAverage]

    println(s"The number of lines with text: ${wikitextsDataset3.count()}")
    println(s"The total number of words not containing digits: ${wordsDataset3.count()}")
    println(s"First row in wordCountDataset: word: '${wordCountDataset3.first().word}', count: ${wordCountDataset3.first().count}")
    println(s"The most common 8-letter word that is not 'software' and starts with 's': '${askedWordCount3.word}' (appears ${askedWordCount3.count} times)")
    println("The average number of times the digits appear in the source data lines:")
    digitDataset3.collect().foreach(digitAverage => println(s"Digit '${digitAverage.digit}': => average: ${digitAverage.average}"))


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

    // 1.
    // - Transformation are operations that create a new DataFrame (or RDD or Dataset) from an existing DataFrame.
    //   The new DataFrame is not computed immediately, but instead a logical plan for the computation is stored.
    // - Actions are operations that force the computation of results from DataFrames. Through actions,
    //   values computed from DataFrames can be stored to variables, shown to the user on screen, or written to a storage.
    //   Unlike transformations, actions propagate immediate computation.
    // - Example transformations: `map`, `filter`, `select`, `groupBy`
    //    - reading data from a source to a DataFrame is also a transformation,
    //      however the existence of the source will be checked immediately
    // - Example actions: `collect`, `count`, `show`, `reduce`
    //
    // 2.
    // - Lazy evaluation means that the computation is not done until required.
    //   Eager evaluation means that the computation is done immediately.
    // - All transformations in Spark are lazily evaluated, while all actions propagate eager evaluation.
    //   This applies regardless of the programming language used, Scala, Python, or some other language.
    //   Due to the lazy evaluation, errors might appear later in the process than expected.
    //     - For example, you can build a large and complex chain of transformations, and they might seem to compute
    //       very fast if you run them without actions (since nothing is actually calculated).
    //       However, when you tried to show or store the results, you then could get an error related to
    //       the first step in the transformation chain.



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

    // AI tool usage and other collaboration should be mentioned here.



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
