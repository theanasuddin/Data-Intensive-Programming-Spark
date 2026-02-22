// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

// The example solutions for exercise 2
package dip25.ex2

// some imports that might be required in the tasks
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Ex2Main extends App {
    // COMP.CS.320 Data-Intensive Programming, Exercise 2
    //
    // This exercise contains basic tasks of data processing using Spark and DataFrames.
    // The last task is a theory question related to the first lecture.
    //
    // This is the Scala version intended for local development.
    //
    // Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    // There is test code and example output following most of the tasks that involve producing code.
    // Uncomment the code in order to run the tests.
    //
    // At the end of the file, there is a question regarding the use of AI or other collaboration when working the tasks.
    // Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle.


    // Some resources that can help with the tasks in this exercise:
    //
    // - The tutorial notebook from our course: in the repository at: /ex1/Basics-of-using-Databricks-notebooks.ipynb
    // - Chapter 3 in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
    //     - There are additional code examples in the related GitHub repository: https://github.com/databricks/LearningSparkV2
    //     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    //       https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc
    // - Databricks tutorial of using Spark DataFrames: https://docs.databricks.com/en/getting-started/dataframes.html
    // - Apache Spark documentation on all available functions that can be used on DataFrames:
    //   https://spark.apache.org/docs/3.5.6/sql-ref-functions.html
    // The full Spark Scala functions API listing for the functions package might have some additional functions listed that
    // have not been updated in the documentation: https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/sql/functions$.html

    // In Databricks, the Spark session is created automatically, and you should not create it yourself.
	val spark: SparkSession = SparkSession
        .builder()
        .appName("ex2-solution")
        .config("spark.driver.host", "localhost")
        .master("local")
        .getOrCreate()

    // suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())



    printTaskLine(1)
    // Task 1 - First Spark task
    //
    // Create and display a DataFrame with your own data, similarly as was done in the tutorial notebook.
    // Your data should have at least 3 columns and at least 5 rows.

    // statistics from Wikipedia: https://en.wikipedia.org/wiki/FIFA_World_Cup#Teams_reaching_the_top_four
    val FIFAWorldCupData = Seq(
        ("Brazil", 5, 23),
        ("Germany", 4, 18),
        ("Italy", 4, 14),
        ("Argentina", 3, 14),
        ("France", 2, 16),
        ("Uruguay", 2, 15),
        ("England", 1, 17),
        ("Spain", 1, 16)
    )
    val worldCupDF: DataFrame = spark
        .createDataFrame(FIFAWorldCupData)
        .toDF("Team", "Titles", "Appearances")

    worldCupDF.show()


    // Example output:
    // ===============
    // +---------+------+-----------+
    // |     Team|Titles|Appearances|
    // +---------+------+-----------+
    // |   Brazil|     5|         23|
    // |  Germany|     4|         18|
    // |    Italy|     4|         14|
    // |Argentina|     3|         14|
    // |   France|     2|         16|
    // |  Uruguay|     2|         15|
    // |  England|     1|         17|
    // |    Spain|     1|         16|
    // +---------+------+-----------+



    printTaskLine(2)
    // Task 2 - Loading CSV data
    //
    // The CSV file "numbers.csv" contains some data on how to spell numbers in different languages.
    // The file is located in the "data/ex2/numbers" folder in the weekly-exercises repository.
    //
    // - Load the data from the file into a DataFrame (numberDF) and display it.
    // - Calculate the number of rows in the DataFrame using numberDF.

    val csvLocation: String = "../../data/ex2/numbers/numbers.csv"
    val numberDF: DataFrame = spark
        .read
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .csv(csvLocation)

    numberDF.show()

    val numberOfNumbers: Long = numberDF.count()

    println(s"Number of rows in the number DataFrame: ${numberOfNumbers}")


    // Example output:
    // ===============
    // +------+-------+---------+-------+------+-------+------+-------+
    // |number|English|  Finnish|Swedish|German|Spanish|French|Italian|
    // +------+-------+---------+-------+------+-------+------+-------+
    // |     1|    one|     yksi|    ett|  eins|    uno|    un|    uno|
    // |     2|    two|    kaksi|    twå|  zwei|    dos|  deux|    due|
    // |     3|  three|    kolme|    tre|  drei|   tres| trois|    tre|
    // |     4|   four|    neljä|   fyra|  vier| cuatro|quatre|quattro|
    // |     5|   five|    viisi|    fem|  fünf|  cinco|  cinq| cinque|
    // |     6|    six|    kuusi|    sex| sechs|   seis|   six|    sei|
    // |     7|  seven|seitsemän|    sju|sieben|  siete|  sept|  sette|
    // |     8|  eight|kahdeksan|   åtta|  acht|   ocho|  huit|   otto|
    // |     9|   nine| yhdeksän|    nio|  neun|  nueve|  neuf|   nove|
    // |    10|    ten| kymmenen|    tio|  zehn|   diez|   dix|  dieci|
    // +------+-------+---------+-------+------+-------+------+-------+
    //
    // and
    //
    // Number of rows in the number DataFrame: 10



    printTaskLine(3)
    // Task 3 - Weather data and the first rows from DataFrame
    //
    // In the "data/ex2/weather" folder is file "nordics_weather.csv" that contains weather data from Finland, Sweden, and Norway in CSV format.
    //
    // The data is based on a dataset from Kaggle: https://www.kaggle.com/datasets/adamwurdits/finland-norway-and-sweden-weather-data-20152019
    // The Kaggle page has further descriptions on the data and the units used in the data.
    //
    // Part 1:
    // - Read the data from the CSV file into DataFrame called "weatherDF". Let Spark infer the schema for the data.
    //   Note that the column separator in the CSV file is a semicolon (";") instead of the default comma.
    // - Print out the schema of the created DataFrame. Study the schema and compare it to the data in the CSV file. Do they match?
    //
    // Part 2:
    // - Fetch the first five rows of the weather data frame and print their contents.

    val weatherLocation: String = "../../data/ex2/weather/nordics_weather.csv"
    val weatherDF: DataFrame = spark
        .read
        .format("csv")
        .option("delimiter", ";")
        .option("header", true)
        .option("inferSchema", true)
        .load(weatherLocation)

    // Code that prints out the schema for weatherDF
    weatherDF.printSchema()

    // It seems that column types have been recognized correctly, i.e., the country as string, date as date, and the others as double
    // => Yes, in this case the inferred schema matches the data


    val weatherSample: Array[Row] = weatherDF.take(5)

    println("The first five rows of the weather data:")
    weatherSample.foreach(row => println(row))


    // Example output:
    // ===============
    // root
    // |-- country: string (nullable = true)
    // |-- date: date (nullable = true)
    // |-- temperature_avg: double (nullable = true)
    // |-- temperature_min: double (nullable = true)
    // |-- temperature_max: double (nullable = true)
    // |-- precipitation: double (nullable = true)
    // |-- snow_depth: double (nullable = true)
    //
    // and
    //
    // The first five rows of the weather data:
    // [Finland,2019-12-28,-9.107407407,-15.28888889,-4.703947368,0.789265537,116.4210526]
    // [Finland,2015-04-08,4.025,1.336129032,6.196129032,0.116666667,486.5833333]
    // [Sweden,2018-10-20,5.077777778,1.241743119,9.210550459,0.885153584,0.0]
    // [Finland,2016-03-07,-0.775,-2.065584416,0.001315789,2.122613065,469.6315789]
    // [Sweden,2017-11-29,-1.355555556,-7.81146789,-3.817889908,2.728667791,103.3424658]



    printTaskLine(4)
    // Task 4 - Minimum and maximum
    //
    // Find the minimum and the maximum temperature from the whole weather data.

    // collecting everything after select, taking the first row and column, and converting to Double
    val minTemp: Double = weatherDF.select(min("temperature_min")).collect()(0)(0).asInstanceOf[Double]
    val maxTemp: Double = weatherDF.select(max("temperature_max")).collect()(0)(0).asInstanceOf[Double]


    // some alternative ways:
    val minTemp2: Double = weatherDF.select(min("temperature_min")).take(1)(0).getAs[Double](0)
    val maxTemp2: Double = weatherDF.select(max("temperature_max")).head().getDouble(0)


    // alternative using SQL syntax and assigning both values with a single query
    weatherDF.createOrReplaceTempView("weatherRelation")
    val (minTempSql: Double, maxTempSql: Double) =
        spark.sql("""
            SELECT min(temperature_min), max(temperature_max)
            FROM weatherRelation
        """)
        .collect()
        .map(row => (row.getDouble(0), row.getDouble(1)))
        .head


    println(s"Minimum temperature is ${minTemp}")
    println(s"Maximum temperature is ${maxTemp}")


    // Example output:
    // ===============
    // Minimum temperature is -29.63961039
    // Maximum temperature is 30.56143791



    printTaskLine(5)
    // Task 5 - Adding columns
    //
    // Add new columns, "measurement_year" and "measurement_weekday", to the weather DataFrame and print out the schema for the new DataFrame.
    //
    // - The new "measurement_year" column should contain the year as an integer based on the value in column "date".
    // - The new "measurement_weekday" column should contain an integer representing the weekday based on column "date".
    //   (0 for Monday, 1 for Tuesday, 2 for Wednesday, ..., 6 for Sunday)

    val weatherDFWithNewColumns: DataFrame = weatherDF
        .withColumn("measurement_year", year(col("date")))
        .withColumn("measurement_weekday", weekday(col("date")))


    // or
    val _2 = weatherDF
        .withColumn("measurement_year", year(weatherDF.col("date")))
        .withColumn("measurement_weekday", weekday(weatherDF.col("date")))


    // alternative using SQL syntax
    weatherDF.createOrReplaceTempView("weatherRelation2")
    val _3: DataFrame = spark.sql("""
        SELECT *, year(date) as measurement_year, weekday(date) as measurement_weekday
        FROM weatherRelation2
    """)


    // code that prints out the schema for weatherDFWithNewColumns
    weatherDFWithNewColumns.printSchema()


    // Example output:
    // ===============
    // root
    // |-- country: string (nullable = true)
    // |-- date: date (nullable = true)
    // |-- temperature_avg: double (nullable = true)
    // |-- temperature_min: double (nullable = true)
    // |-- temperature_max: double (nullable = true)
    // |-- precipitation: double (nullable = true)
    // |-- snow_depth: double (nullable = true)
    // |-- measurement_year: integer (nullable = true)
    // |-- measurement_weekday: integer (nullable = true)



    printTaskLine(6)
    // Task 6 - Aggregated DataFrame
    //
    // Find the minimum and the maximum temperature for each year.
    //
    // Sort the resulting DataFrame based on year so that the latest year is the first row in the DataFrame.

    val yearlyTemperatureDF: DataFrame = weatherDFWithNewColumns
        .groupBy(col("measurement_year").as("year"))
        .agg(
            min("temperature_min").alias("temperature_min"),
            max("temperature_max").alias("temperature_max")
        )
        .orderBy(col("year").desc)


    // another way using SQL syntax
    weatherDFWithNewColumns.createOrReplaceTempView("weatherDFWithNewColumnsRelation")
    val sqlTemperatureDF: DataFrame = spark.sql("""
        SELECT measurement_year AS year, min(temperature_min) AS temperature_min, max(temperature_max) AS temperature_max
        FROM weatherDFWithNewColumnsRelation
        GROUP BY year
        ORDER BY year DESC
    """)


    // another option using inner join (likely less efficient than the other options)
    val sqlTemperatureDF2: DataFrame = weatherDFWithNewColumns
        .groupBy("measurement_year")
        .agg(min("temperature_min").alias("temperature_min"))
        .join(
            weatherDFWithNewColumns.groupBy("measurement_year").agg(max("temperature_max").alias("temperature_max")),
            Seq("measurement_year"),
            "inner"
        )
        .withColumnRenamed("measurement_year", "year")
        .orderBy(col("year").desc)


    yearlyTemperatureDF.show()


    // Example output:
    // ===============
    // +----+---------------+---------------+
    // |year|temperature_min|temperature_max|
    // +----+---------------+---------------+
    // |2019|   -26.63708609|    29.47627907|
    // |2018|   -24.00592105|    30.56143791|
    // |2017|        -24.922|    23.14771242|
    // |2016|   -29.63961039|    26.28026906|
    // |2015|   -21.97961783|     25.7285124|
    // +----+---------------+---------------+



    printTaskLine(7)
    // Task 7 - Average measurements in Finland
    //
    // Using the weather data, find out the average precipitation (in cm) and
    // the average snow depth (in mm) for each week day in Finland in year 2017.
    // Round both of the averages to 2 decimals.

    val avgFinlandDF: DataFrame = weatherDFWithNewColumns
        .filter(col("country") === "Finland" && col("measurement_year") === 2017)
        .groupBy(col("measurement_weekday").as("weekday"))
        .agg(
            round(avg("precipitation"), 2).as("precipitation_avg"),
            round(avg("snow_depth"), 2).as("snow_depth_avg")
        )
        .orderBy(col("weekday").asc)


    // the same using SQL syntax
    weatherDFWithNewColumns.createOrReplaceTempView("task7Relation")
    val avgFinlandSqlDF: DataFrame = spark.sql("""
        SELECT
            measurement_weekday AS weekday,
            round(avg(precipitation), 2) AS precipitation_avg,
            round(avg(snow_depth), 2) AS snow_depth_avg
        FROM task7Relation
        WHERE country = 'Finland' AND measurement_year = 2017
        GROUP BY weekday
        ORDER BY weekday ASC
    """)


    avgFinlandDF.show()


    // Example output:
    // ===============
    // +-------+-----------------+--------------+
    // |weekday|precipitation_avg|snow_depth_avg|
    // +-------+-----------------+--------------+
    // |      0|             1.61|        213.49|
    // |      1|             2.19|         222.1|
    // |      2|             1.63|        217.13|
    // |      3|             1.99|        222.12|
    // |      4|             1.45|        214.12|
    // |      5|             1.84|        222.39|
    // |      6|             1.73|        212.79|
    // +-------+-----------------+--------------+



    printTaskLine(8)
    // Task 8 - Scaling in data engineering
    //
    // Using your own words, answer the following questions:
    //
    // 1. What is meant by "scaling" in the context of data engineering?
    // 2. What is the difference between horizontal and vertical scaling?
    // 3. What benefits can be achieved by scaling?
    // 4. What kind of problems can be encountered related to scaling?
    //
    // Extensive answers are not required here.
    // If your answers do not fit into one screen, you have likely written more than what was expected.

    // 1. Scaling means adjusting the available computing resources according to the amount of data.
    //     - Computing resources: memory (RAM), CPU, number of machines (nodes), etc.
    //     - Amount of data: volume, velocity, etc.
    // 2. Horizontal scaling means adding (or removing) more machines to the resource pool.
    //    Vertical scaling means increasing or decreasing the resources (RAM, CPU) of the existing machines.
    // 3. Scaling up can improve performance, and allow the handling of larger or more complex datasets.
    //    Scaling down can reduce the incurred costs. In optimal situation, the number of resources match demand to optimize the costs.
    // 4. Scaling can introduce complexity and higher costs if it is not managed well.
    //    Inefficient scaling can cause bottlenecks to the system.



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
