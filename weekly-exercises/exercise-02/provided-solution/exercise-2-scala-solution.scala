// Databricks notebook source
// MAGIC %md
// MAGIC Copyright 2025 Tampere University<br>
// MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
// MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
// MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

// COMMAND ----------

// MAGIC %md
// MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 2
// MAGIC
// MAGIC This exercise contains basic tasks of data processing using Spark and DataFrames. The last task is a theory question related to the first lecture.
// MAGIC
// MAGIC This is the **Scala** version, switch to the Python version if you want to do the tasks in Python.
// MAGIC
// MAGIC Each task has its own cell(s) for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary.<br>
// MAGIC There are cells with test code and example output following most of the tasks that involve producing code.
// MAGIC
// MAGIC At the end of the notebook, there is a question regarding the use of AI or other collaboration when working the tasks.<br>
// MAGIC Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle: [Weekly Exercise #2](https://moodle.tuni.fi/mod/assign/view.php?id=3503817)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Some resources that can help with the tasks in this exercise:
// MAGIC
// MAGIC - The [tutorial notebook](https://adb-7895492183558578.18.azuredatabricks.net/editor/notebooks/743402606902162) from our course
// MAGIC - Chapter 3 in [Learning Spark, 2nd Edition](https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
// MAGIC     - There are additional code examples in the related [GitHub repository](https://github.com/databricks/LearningSparkV2).
// MAGIC     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL<br> `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
// MAGIC - [Databricks tutorial](https://docs.databricks.com/en/getting-started/dataframes.html) of using Spark DataFrames
// MAGIC - [Apache Spark documentation](https://spark.apache.org/docs/3.5.6/sql-ref-functions.html) on all available functions that can be used on DataFrames.<br>
// MAGIC   The full [Spark Scala functions API listing](https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/sql/functions$.html) for the functions package might have some additional functions listed that have not been updated in the documentation.

// COMMAND ----------

// some imports that might be required in the tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 1 - First Spark task
// MAGIC
// MAGIC Create and display a DataFrame with your own data, similarly as was done in the tutorial notebook.<br>
// MAGIC Your data should have at least 3 columns and at least 5 rows.

// COMMAND ----------

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

// The Databricks specific display command must be the last one in the cell.
display(worldCupDF)

// COMMAND ----------

// The general show method can also be used outside of Databricks, but it works here as well.
// All example outputs for DataFrames in the exercise notebooks are generated using show.
worldCupDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC (the actual data can be totally different):
// MAGIC
// MAGIC ```text
// MAGIC +---------+------+-----------+
// MAGIC |     Team|Titles|Appearances|
// MAGIC +---------+------+-----------+
// MAGIC |   Brazil|     5|         23|
// MAGIC |  Germany|     4|         18|
// MAGIC |    Italy|     4|         14|
// MAGIC |Argentina|     3|         14|
// MAGIC |   France|     2|         16|
// MAGIC |  Uruguay|     2|         15|
// MAGIC |  England|     1|         17|
// MAGIC |    Spain|     1|         16|
// MAGIC +---------+------+-----------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 2 - Loading CSV data
// MAGIC
// MAGIC The CSV file `numbers.csv` contains some data on how to spell numbers in different languages. The file is located in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) in folder `exercises/ex2/numbers`.
// MAGIC
// MAGIC As a reminder from the tutorial notebook: the path to be used is of the form: `abfss://<container>@tunics320f2025gen2.dfs.core.windows.net/<path>/<to>/<file_or_folder>`<br>
// MAGIC where `<container>` is either `shared` (for reading data) or `students` (for reading and writing data).
// MAGIC
// MAGIC - Load the data from the file into a DataFrame (`numberDF`) and display it.
// MAGIC - Calculate the number of rows in the DataFrame using `numberDF`.

// COMMAND ----------

val csvLocation: String = "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex2/numbers/numbers.csv"
val numberDF: DataFrame = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv(csvLocation)

numberDF.show()

// COMMAND ----------

val numberOfNumbers: Long = numberDF.count()

println(s"Number of rows in the number DataFrame: ${numberOfNumbers}")
println("==========================================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC +------+-------+---------+-------+------+-------+------+-------+
// MAGIC |number|English|  Finnish|Swedish|German|Spanish|French|Italian|
// MAGIC +------+-------+---------+-------+------+-------+------+-------+
// MAGIC |     1|    one|     yksi|    ett|  eins|    uno|    un|    uno|
// MAGIC |     2|    two|    kaksi|    twå|  zwei|    dos|  deux|    due|
// MAGIC |     3|  three|    kolme|    tre|  drei|   tres| trois|    tre|
// MAGIC |     4|   four|    neljä|   fyra|  vier| cuatro|quatre|quattro|
// MAGIC |     5|   five|    viisi|    fem|  fünf|  cinco|  cinq| cinque|
// MAGIC |     6|    six|    kuusi|    sex| sechs|   seis|   six|    sei|
// MAGIC |     7|  seven|seitsemän|    sju|sieben|  siete|  sept|  sette|
// MAGIC |     8|  eight|kahdeksan|   åtta|  acht|   ocho|  huit|   otto|
// MAGIC |     9|   nine| yhdeksän|    nio|  neun|  nueve|  neuf|   nove|
// MAGIC |    10|    ten| kymmenen|    tio|  zehn|   diez|   dix|  dieci|
// MAGIC +------+-------+---------+-------+------+-------+------+-------+
// MAGIC ```
// MAGIC and
// MAGIC ```
// MAGIC Number of rows in the number DataFrame: 10
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 3 - Weather data and the first rows from DataFrame
// MAGIC
// MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) in the `exercises/ex2/weather` folder is file `nordics_weather.csv` that contains weather data from Finland, Sweden, and Norway in CSV format.
// MAGIC
// MAGIC The data is based on a dataset from Kaggle: [Finland, Norway, and Sweden Weather Data 2015-2019](https://www.kaggle.com/datasets/adamwurdits/finland-norway-and-sweden-weather-data-20152019).
// MAGIC The Kaggle page has further descriptions on the data and the units used in the data.
// MAGIC
// MAGIC Part 1:
// MAGIC
// MAGIC - Read the data from the CSV file into DataFrame called `weatherDF`. Let Spark infer the schema for the data.<br>
// MAGIC   Note that the column separator in the CSV file is a semicolon (`;`) instead of the default comma.
// MAGIC - Print out the schema of the created DataFrame. Study the schema and compare it to the data in the CSV file. Do they match?
// MAGIC
// MAGIC Part 2:
// MAGIC
// MAGIC - Fetch the first five rows of the weather data frame and print their contents.

// COMMAND ----------

val weatherLocation: String = "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex2/weather/nordics_weather.csv"
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

// COMMAND ----------

val weatherSample: Array[Row] = weatherDF.take(5)

println("The first five rows of the weather data:")
weatherSample.foreach(row => println(row))
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC root
// MAGIC  |-- country: string (nullable = true)
// MAGIC  |-- date: date (nullable = true)
// MAGIC  |-- temperature_avg: double (nullable = true)
// MAGIC  |-- temperature_min: double (nullable = true)
// MAGIC  |-- temperature_max: double (nullable = true)
// MAGIC  |-- precipitation: double (nullable = true)
// MAGIC  |-- snow_depth: double (nullable = true)
// MAGIC ```
// MAGIC
// MAGIC and
// MAGIC
// MAGIC ```text
// MAGIC The first five rows of the weather data:
// MAGIC [Finland,2019-12-28,-9.107407407,-15.28888889,-4.703947368,0.789265537,116.4210526]
// MAGIC [Finland,2015-04-08,4.025,1.336129032,6.196129032,0.116666667,486.5833333]
// MAGIC [Sweden,2018-10-20,5.077777778,1.241743119,9.210550459,0.885153584,0.0]
// MAGIC [Finland,2016-03-07,-0.775,-2.065584416,0.001315789,2.122613065,469.6315789]
// MAGIC [Sweden,2017-11-29,-1.355555556,-7.81146789,-3.817889908,2.728667791,103.3424658]
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 4 - Minimum and maximum
// MAGIC
// MAGIC Find the minimum and the maximum temperature from the whole weather data.

// COMMAND ----------

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

// COMMAND ----------

println(s"Minimum temperature is ${minTemp}")
println(s"Maximum temperature is ${maxTemp}")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC Minimum temperature is -29.63961039
// MAGIC Maximum temperature is 30.56143791
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 5 - Adding columns
// MAGIC
// MAGIC Add new columns, `measurement_year` and `measurement_weekday`, to the weather DataFrame and print out the schema for the new DataFrame.
// MAGIC
// MAGIC - The new `measurement_year` column should contain the year as an integer based on the value in column `date`.
// MAGIC - The new `measurement_weekday` column should contain an integer representing the weekday based on column `date`.<br>
// MAGIC   (0 for Monday, 1 for Tuesday, 2 for Wednesday, ..., 6 for Sunday)

// COMMAND ----------

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

// COMMAND ----------

// code that prints out the schema for weatherDFWithNewColumns
weatherDFWithNewColumns.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC root
// MAGIC  |-- country: string (nullable = true)
// MAGIC  |-- date: date (nullable = true)
// MAGIC  |-- temperature_avg: double (nullable = true)
// MAGIC  |-- temperature_min: double (nullable = true)
// MAGIC  |-- temperature_max: double (nullable = true)
// MAGIC  |-- precipitation: double (nullable = true)
// MAGIC  |-- snow_depth: double (nullable = true)
// MAGIC  |-- measurement_year: integer (nullable = true)
// MAGIC  |-- measurement_weekday: integer (nullable = true)
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 6 - Aggregated DataFrame
// MAGIC
// MAGIC Find the minimum and the maximum temperature for each year.
// MAGIC
// MAGIC Sort the resulting DataFrame based on year so that the latest year is the first row in the DataFrame.

// COMMAND ----------

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

// COMMAND ----------

yearlyTemperatureDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC +----+---------------+---------------+
// MAGIC |year|temperature_min|temperature_max|
// MAGIC +----+---------------+---------------+
// MAGIC |2019|   -26.63708609|    29.47627907|
// MAGIC |2018|   -24.00592105|    30.56143791|
// MAGIC |2017|        -24.922|    23.14771242|
// MAGIC |2016|   -29.63961039|    26.28026906|
// MAGIC |2015|   -21.97961783|     25.7285124|
// MAGIC +----+---------------+---------------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 7 - Average measurements in Finland
// MAGIC
// MAGIC Using the weather data, find out the average precipitation (in cm) and the average snow depth (in mm) for each week day in Finland in year 2017.<br>
// MAGIC Round both of the averages to 2 decimals.

// COMMAND ----------

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

// COMMAND ----------

avgFinlandDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC +-------+-----------------+--------------+
// MAGIC |weekday|precipitation_avg|snow_depth_avg|
// MAGIC +-------+-----------------+--------------+
// MAGIC |      0|             1.61|        213.49|
// MAGIC |      1|             2.19|         222.1|
// MAGIC |      2|             1.63|        217.13|
// MAGIC |      3|             1.99|        222.12|
// MAGIC |      4|             1.45|        214.12|
// MAGIC |      5|             1.84|        222.39|
// MAGIC |      6|             1.73|        212.79|
// MAGIC +-------+-----------------+--------------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 8 - Scaling in data engineering
// MAGIC
// MAGIC Using your own words, answer the following questions:
// MAGIC
// MAGIC 1. What is meant by "scaling" in the context of data engineering?
// MAGIC 2. What is the difference between horizontal and vertical scaling?
// MAGIC 3. What benefits can be achieved by scaling?
// MAGIC 4. What kind of problems can be encountered related to scaling?
// MAGIC
// MAGIC Extensive answers are not required here.<br>
// MAGIC If your answers do not fit into one screen, you have likely written more than what was expected.

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Scaling means adjusting the available computing resources according to the amount of data.
// MAGIC     - Computing resources: memory (RAM), CPU, number of machines (nodes), etc.
// MAGIC     - Amount of data: volume, velocity, etc.
// MAGIC 2. Horizontal scaling means adding (or removing) more machines to the resource pool. Vertical scaling means increasing or decreasing the resources (RAM, CPU) of the existing machines.
// MAGIC 3. Scaling up can improve performance, and allow the handling of larger or more complex datasets. Scaling down can reduce the incurred costs. In optimal situation, the number of resources match demand to optimize the costs.
// MAGIC 4. Scaling can introduce complexity and higher costs if it is not managed well. Inefficient scaling can cause bottlenecks to the system.

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
// MAGIC AI tool usage and other collaboration should be mentioned here.
