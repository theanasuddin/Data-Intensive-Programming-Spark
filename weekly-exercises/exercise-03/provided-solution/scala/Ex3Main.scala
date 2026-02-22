// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

// The example solutions for exercise 3
package dip25.ex3

// some imports that might be required in the tasks
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, DateType, DoubleType, StringType, StructField, StructType}


object Ex3Main extends App {
    // COMP.CS.320 Data-Intensive Programming, Exercise 3
    //
    // This exercise contains additional data processing tasks using DataFrames.
    // The last task is a theory question related to the lectures.<br>
    // The coding tasks can be slightly more challenging than those in exercise 2.
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
    // - Chapter 3 and 5 (Section: Common DataFrames and Spark SQL Operations) in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
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
        .appName("ex3-solution")
        .config("spark.driver.host", "localhost")
        .master("local")
        .getOrCreate()

    // suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())



    printTaskLine(1)
    // Task 1 - Load new data to DataFrames
    //
    // The data used in tasks 1-5 is weather data that is based on a dataset from Kaggle:
    // https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data
    //
    // In the weekly-exercises repository, the "data/ex3/weather" folder contains
    // two subfolders: "measurements" and "metadata".
    //
    //  - "measurements" folder contains daily weather data in Parquet format in file "daily_weather.parquet".
    //  - "metadata" folder contains two separate CSV files, "cities.csv" and "countries.csv"
    //
    // Some columns and rows have been removed for this task. For the remaining columns,
    // the Kaggle page has further descriptions on the data.
    //
    // Parquet is a column-oriented data file format designed for efficient data storage and retrieval.
    // Unlike CSV files, the data given in Parquet format is not as easy to preview without Spark.
    // But if you really want, for example, the Parquet Visualizer,
    // https://marketplace.visualstudio.com/items?itemName=lucien-martijn.parquet-visualizer,
    // Visual Studio Code extension can be used to browse the data contained in a Parquet file.
    // However, understanding the format is not important for this exercise.
    // The format will be discussed in more detail during the lectures later in the course.
    //
    // The task:
    // - Load the daily weather data into DataFrame "weatherDF".
    // - Load the city related metadata into DataFrame "cityDF".
    // - Load the country related metadata into DataFrame "countryDF".

    val basePath: String = "../../data/ex3/weather"
    val measurementPath: String = s"${basePath}/measurements"
    val cityFilePath: String = s"${basePath}/metadata/cities.csv"
    val countryFilePath: String = s"${basePath}/metadata/countries.csv"

    val weatherDF: DataFrame = spark
        .read
        .parquet(measurementPath)  // if the given path is a folder, Spark reads all the files from the folder

    val cityDF: DataFrame = spark
        .read
        .option("header", "true")
        .option("sep", ",")
        .option("inferSchema", "true")
        .csv(cityFilePath)

    val countryDF: DataFrame = spark
        .read
        .option("header", "true")
        .option("sep", ",")
        .option("inferSchema", "true")
        .csv(countryFilePath)

    // alternative version providing explicit schema for the CSV data to avoid an extra passthrough of the CSV file to infer the schema
    val citySchema: StructType = new StructType(
        Array(
            StructField("station_id", StringType, true),
            StructField("city_name", StringType, true),
            StructField("iso2", StringType, true),
            StructField("iso3", StringType, true),
            StructField("latitude", DoubleType, true),
            StructField("longitude", DoubleType, true)
        )
    )
    val countrySchema: StructType = new StructType(
        Array(
            StructField("country", StringType, true),
            StructField("native_name", StringType, true),
            StructField("iso2", StringType, true),
            StructField("iso3", StringType, true),
            StructField("population", DoubleType, true),
            StructField("area", DoubleType, true),
            StructField("capital", StringType, true),
            StructField("region", StringType, true),
            StructField("continent", StringType, true)
        )
    )
    val cityDF2: DataFrame = spark
        .read
        .option("header", "true")
        .option("sep", ",")
        .schema(citySchema)
        .csv(cityFilePath)
    val countryDF2: DataFrame = spark
        .read
        .option("header", "true")
        .option("sep", ",")
        .schema(countrySchema)
        .csv(countryFilePath)


    println("The first 5 rows of the weather data")
    weatherDF.limit(5).show()

    println("The first 5 rows of the city data")
    cityDF.limit(5).show()

    println("The first 5 rows of the country data")
    countryDF.limit(5).show()


    // Example output:
    // ===============
    // The first 5 rows of the weather data
    // +----------+----------+----------+----------+----------------+
    // |station_id|      date|min_temp_c|max_temp_c|precipitation_mm|
    // +----------+----------+----------+----------+----------------+
    // |     01008|2000-01-01|      -6.7|      -0.8|             1.0|
    // |     01026|2000-01-01|      -6.1|      -1.6|             0.0|
    // |     01271|2000-01-01|      -5.1|       4.4|             2.0|
    // |     01465|2000-01-01|       1.8|       6.5|             0.6|
    // |     01492|2000-01-01|      -8.6|      -3.2|             6.0|
    // +----------+----------+----------+----------+----------------+
    // The first 5 rows of the city data
    // +----------+----------+----+----+-------------+-------------+
    // |station_id| city_name|iso2|iso3|     latitude|    longitude|
    // +----------+----------+----+----+-------------+-------------+
    // |     41515|  Asadabad|  AF| AFG|34.8660000397|71.1500045859|
    // |     38954|  Fayzabad|  AF| AFG|37.1297607616|70.5792471913|
    // |     41560| Jalalabad|  AF| AFG|34.4415269155|70.4361034738|
    // |     38947|    Kunduz|  AF| AFG|36.7279506623|68.8725296619|
    // |     38987|Qala i Naw|  AF| AFG| 34.983000131|63.1332996367|
    // +----------+----------+----+----+-------------+-------------+
    // The first 5 rows of the country data
    // +--------------+--------------+----+----+----------+---------+----------------+--------------------+---------+
    // |       country|   native_name|iso2|iso3|population|     area|         capital|              region|continent|
    // +--------------+--------------+----+----+----------+---------+----------------+--------------------+---------+
    // |   Afghanistan|     افغانستان|  AF| AFG| 2.60231E7| 652230.0|           Kabul|Southern and Cent...|     Asia|
    // |       Albania|     Shqipëria|  AL| ALB| 2895947.0|  28748.0|          Tirana|     Southern Europe|   Europe|
    // |       Algeria|       الجزائر|  DZ| DZA|    3.87E7|2381741.0|         Algiers|     Northern Africa|   Africa|
    // |American Samoa|American Samoa|  AS| ASM|   55519.0|    199.0|       Pago Pago|           Polynesia|  Oceania|
    // |       Andorra|          NULL|  AD| AND|   87486.0|   4678.0|Andorra la Vella|     Southern Europe|   Europe|
    // +--------------+--------------+----+----+----------+---------+----------------+--------------------+---------+



    printTaskLine(2)
    // Task 2 - Combine data into a single DataFrame
    //
    // Part 1:
    // - Create a new `metadataDF` DataFrame where the country information from `countryDF` has been added to `cityDF`.
    // - The new DataFrame should have the same number of rows as the original `cityDF`.
    // - Hint: either `iso2` or `iso3` column can be used to connect the city and country data.
    //
    // Part 2:
    // - Create a new `fullDF` DataFrame where the metadata from `metadataDF` has been added to the weather measurements `weatherDF`.
    // - The new DataFrame should have the same number of rows as the original `weatherDF`.
    //
    // In the example outputs, all columns that will not be needed in the following tasks have been removed.
    // Removing the columns here is allowed, but not compulsory.

    val metadataDF: DataFrame = cityDF
        .drop("iso2", "longitude")
        .join(
            countryDF
                .drop("iso2", "native_name", "population", "area", "region", "continent"),
            Seq("iso3"),
            "inner"
        )
        .drop("iso3")

    val fullDF: DataFrame = weatherDF
        .join(metadataDF, Seq("station_id"), "inner")


    metadataDF.limit(8).show(false)
    println(s"Number of rows in the metadata DataFrame: ${metadataDF.count()}")

    fullDF.limit(8).show(false)
    println(s"Number of rows in the full DataFrame: ${fullDF.count()}")


    // Example output:
    // ===============
    // (depending how the data combination is done, the first data rows might not be the same as shown here)
    // (in these example outputs, columns that will not be needed in later tasks have been removed)
    //
    // +----------+----------+-------------+-----------+-------+
    // |station_id|city_name |latitude     |country    |capital|
    // +----------+----------+-------------+-----------+-------+
    // |41515     |Asadabad  |34.8660000397|Afghanistan|Kabul  |
    // |38954     |Fayzabad  |37.1297607616|Afghanistan|Kabul  |
    // |41560     |Jalalabad |34.4415269155|Afghanistan|Kabul  |
    // |38947     |Kunduz    |36.7279506623|Afghanistan|Kabul  |
    // |38987     |Qala i Naw|34.983000131 |Afghanistan|Kabul  |
    // |38915     |Sheberghan|36.6579807729|Afghanistan|Kabul  |
    // |13577     |Peshkopi  |41.6833020982|Albania    |Tirana |
    // |13461     |Shkodër   |42.0684515575|Albania    |Tirana |
    // +----------+----------+-------------+-----------+-------+
    // Number of rows in the metadata DataFrame: 1208
    // +----------+----------+----------+----------+----------------+-----------+-------------+-----------+--------+
    // |station_id|date      |min_temp_c|max_temp_c|precipitation_mm|city_name  |latitude     |country    |capital |
    // +----------+----------+----------+----------+----------------+-----------+-------------+-----------+--------+
    // |06660     |2000-01-01|-0.3      |2.7       |1.4             |Zürich     |47.3799878124|Switzerland|Bern    |
    // |31713     |2000-01-01|-31.5     |-17.0     |0.0             |Birobidzhan|48.7974206737|Russia     |Moscow  |
    // |82993     |2000-01-01|22.6      |31.0      |0.0             |Maceió     |-9.6199955049|Brazil     |Brasília|
    // |06660     |2000-01-02|0.8       |3.0       |0.0             |Zürich     |47.3799878124|Switzerland|Bern    |
    // |60351     |2000-01-02|8.2       |14.7      |29.0            |Jijel      |36.8219970335|Algeria    |Algiers |
    // |78073     |2000-01-02|21.6      |26.7      |0.0             |Nassau     |25.0833901154|The Bahamas|Nassau  |
    // |82993     |2000-01-02|22.6      |31.2      |0.0             |Maceió     |-9.6199955049|Brazil     |Brasília|
    // |06660     |2000-01-03|-1.7      |2.3       |0.0             |Zürich     |47.3799878124|Switzerland|Bern    |
    // +----------+----------+----------+----------+----------------+-----------+-------------+-----------+--------+
    // Number of rows in the full DataFrame: 5130470



    printTaskLine(3)
    // Task 3 - The coldest and the hottest day in each country
    //
    // Part 1:
    // - Find the lowest temperature and the date it happened for each country.
    // - Order the countries such that the country with coldest temperature is given first.
    // - Store the result in `coldDayDF` DataFrame.
    //
    // Part 2:
    // - Find the highest temperature and the date it happened for each country.
    // - Order the countries such that the country with hottest temperature is given first.
    // - Store the result in `hotDayDF` DataFrame.

    val extremeDayDF: DataFrame = fullDF
        .groupBy("country")
        .agg(
            min("min_temp_c").alias("coldest_temp"),
            min_by(col("date"), col("min_temp_c")).alias("coldest_date"),
            max("max_temp_c").alias("hottest_temp"),
            max_by(col("date"), col("max_temp_c")).alias("hottest_date")
        )

    val coldDayDF: DataFrame = extremeDayDF
        .select(col("country"), col("coldest_temp"), col("coldest_date").as("date"))
        .orderBy(col("coldest_temp").asc, col("date").asc)

    val hotDayDF: DataFrame = extremeDayDF
        .select(col("country"), col("hottest_temp"), col("hottest_date").as("date"))
        .orderBy(col("hottest_temp").desc, col("date").asc)


    coldDayDF.limit(10).show()

    hotDayDF.limit(10).show()


    // Example output:
    // ===============
    //  +-------------+------------+----------+
    //  |      country|coldest_temp|      date|
    //  +-------------+------------+----------+
    //  |       Russia|       -55.4|2000-01-07|
    //  |       Canada|       -46.6|2005-01-13|
    //  |     Mongolia|       -46.6|2005-02-09|
    //  |      Tunisia|       -45.1|2023-04-01|
    //  |     Dominica|       -44.9|2005-01-17|
    //  |   Kazakhstan|       -42.6|2001-01-07|
    //  |United States|       -42.2|2009-01-15|
    //  |       Latvia|       -41.3|2020-01-17|
    //  |   Kyrgyzstan|       -39.7|2023-01-13|
    //  |      Finland|       -39.4|2010-02-20|
    //  +-------------+------------+----------+
    //  +----------+------------+----------+
    //  |   country|hottest_temp|      date|
    //  +----------+------------+----------+
    //  |     Chile|        90.0|2020-04-12|
    //  |    Brazil|        80.0|2021-03-17|
    //  |  Tanzania|        78.0|2020-12-19|
    //  |     Ghana|        60.5|2023-03-13|
    //  | Argentina|        60.0|2018-11-09|
    //  |   Lesotho|        58.0|2023-02-13|
    //  |      Laos|        56.0|2019-05-17|
    //  |Mozambique|        56.0|2022-05-14|
    //  |  Colombia|        56.0|2023-04-17|
    //  |     India|        56.0|2023-06-05|
    //  +----------+------------+----------+
    //
    // Note that some results are not believable and imply partly broken data.
    // For example, Dominica (an island in Caribbean) should not get negative temperatures.
    // And the first few hottest temperatures are too high to be believable.



    printTaskLine(4)
    // Task 4 - Extreme days in Nordic countries
    //
    // In this task only the weather data for the independent Nordic countries
    // ("Finland", "Sweden", "Norway", "Denmark", "Iceland") should be considered.
    //
    // Part 1:
    // - Similarly to task 3, find the coldest and the hottest days (including the dates) for the five Nordic countries.
    // - Also, find the date with the most rain (including the rain amount) for the five Nordic countries.
    // - Store the result in `nordicExtremeDayDF` DataFrame.
    //
    // Part 2:
    // - Repeat part 1, but only consider data that is measured in the capital city of each Nordic country.
    // - Store the result in `nordicCapitalExtremeDayDF` DataFrame.

    val nordicDF: DataFrame = fullDF
        .filter(col("country").isin("Finland", "Sweden", "Norway", "Denmark", "Iceland"))

    val nordicCapitalDF: DataFrame = nordicDF
        .filter(col("city_name") === col("capital"))

    // helper function to avoid repeating code
    def getExtremeDF(inputDF: DataFrame): DataFrame = {
        inputDF
            .groupBy("country")
            .agg(
                min("min_temp_c").alias("coldest_temp"),
                min_by(col("date"), col("min_temp_c")).alias("coldest_date"),
                max("max_temp_c").alias("hottest_temp"),
                max_by(col("date"), col("max_temp_c")).alias("hottest_date"),
                max("precipitation_mm").alias("max_rainfall"),
                max_by(col("date"), col("precipitation_mm")).alias("rainiest_date")
            )
    }

    val nordicExtremeDayDF: DataFrame = getExtremeDF(nordicDF)

    val nordicCapitalExtremeDF: DataFrame = getExtremeDF(nordicCapitalDF)


    println("Extreme days in Nordic countries including measurements from all stations:")
    nordicExtremeDayDF.show()

    println("Extreme days in Nordic countries using only measurements from country capitals:")
    nordicCapitalExtremeDF.show()


    // Example output:
    // ===============
    // Extreme days in Nordic countries including measurements from all stations:
    // +-------+------------+------------+------------+------------+------------+-------------+
    // |country|coldest_temp|coldest_date|hottest_temp|hottest_date|max_rainfall|rainiest_date|
    // +-------+------------+------------+------------+------------+------------+-------------+
    // | Sweden|       -28.9|  2010-01-30|        36.9|  2022-07-21|       102.1|   2022-08-27|
    // |Finland|       -39.4|  2010-02-20|        35.0|  2010-07-29|        86.1|   2004-07-28|
    // | Norway|       -25.1|  2010-02-23|        34.6|  2018-07-27|        90.9|   2022-10-24|
    // |Denmark|       -18.6|  2010-12-21|        36.7|  2022-07-20|       110.0|   2005-12-06|
    // |Iceland|       -15.2|  2022-12-31|        25.7|  2008-07-30|        66.8|   2001-10-31|
    // +-------+------------+------------+------------+------------+------------+-------------+
    // Extreme days in Nordic countries using only measurements from country capitals:
    // +-------+------------+------------+------------+------------+------------+-------------+
    // |country|coldest_temp|coldest_date|hottest_temp|hottest_date|max_rainfall|rainiest_date|
    // +-------+------------+------------+------------+------------+------------+-------------+
    // | Sweden|       -21.0|  2010-02-22|        34.5|  2022-07-21|        44.0|   2014-09-21|
    // |Finland|       -21.2|  2021-01-15|        30.2|  2021-07-15|        62.0|   2019-08-23|
    // | Norway|       -20.7|  2001-02-05|        34.6|  2018-07-27|        72.8|   2014-06-26|
    // |Iceland|       -15.2|  2022-12-31|        25.7|  2008-07-30|        54.6|   2012-12-29|
    // +-------+------------+------------+------------+------------+------------+-------------+
    //
    // There are no measurements from Copenhagen, the capital of Denmark,
    // which is why Denmark is not included in the second output.



    printTaskLine(5)
    // Task 5 - Countries with dry months
    //
    // For this task, a country is considered to have a "fully dry month" only if
    // - there are measurements from the capital of the country for each day of the month
    // - and none of the measurements from the capital show rain, i.e., the precipitation is not larger than 0
    //
    // Note that there is only one row for each (station_id, date)-pair in the weather data.
    // All duplicates in the original data have been removed for this exercise's dataset.
    //
    // Part 1:
    // - Find how many fully dry months each country had in the year 2022.
    // - Store the country name and the dry-month count for those countries that had at least 3 dry months into DataFrame `threeDryMonthsDF`.
    //
    // Part 2:
    // - Find the names of the countries for which the capital is in the Southern Hemisphere (i.e, latitude < 0),
    //   and that had at least one fully dry month in the year 2022.
    // - Store the names as a list of strings to `southernDryCountries`.

    val dryMonthsDF: DataFrame = fullDF
        .filter(col("city_name") === col("capital") && year(col("date")) === 2022)
        .withColumn("month", month(col("date")))
        .withColumn("days_in_month", dayofmonth(last_day(col("date"))))
        .groupBy("country", "month")
        .agg(
            first("days_in_month").alias("days_in_month"),
            count(when(col("precipitation_mm") <= 0, true)).alias("dry_day_count"),
            first(col("latitude")).as("latitude")
        )
        .filter(col("dry_day_count") === col("days_in_month"))
        .groupBy("country")
        .agg(
            count("*").alias("dry_month_count"),
            first(col("latitude")).as("latitude")
        )

    val threeDryMonthsDF: DataFrame = dryMonthsDF
        .filter(col("dry_month_count") >= 3)
        .drop("latitude")
        .orderBy(col("dry_month_count").desc, col("country").asc)

    val southernDryCountries: List[String] = dryMonthsDF
        .filter(col("dry_month_count") >= 1 && col("latitude") < 0)
        .select("country")
        .orderBy(col("country").asc)
        .collect()
        .map(row => row.getString(0))
        .toList


    threeDryMonthsDF.show()

    println("The southern hemisphere countries with at least one fully dry month in 2022:")
    southernDryCountries.foreach(country => println(s"- ${country}"))


    // Example output:
    // ===============
    // +--------------------+---------------+
    // |             country|dry_month_count|
    // +--------------------+---------------+
    // |                Oman|              5|
    // |             Bahrain|              4|
    // |               Qatar|              4|
    // |        Saudi Arabia|              4|
    // |               Syria|              4|
    // |United Arab Emirates|              4|
    // |                Iran|              3|
    // |             Namibia|              3|
    // |          Tajikistan|              3|
    // |            Tanzania|              3|
    // +--------------------+---------------+
    // The southern hemisphere countries with at least one fully dry month in 2022:
    // - Angola
    // - Namibia
    // - Tanzania
    // - Zimbabwe



    printTaskLine(6)
    // Task 6 - Electricity data and hourly averages
    //
    // Background information related to the data source
    //
    // As part of Tampere University research projects ProCem, https://www.senecc.fi/projects/procem-2, and ProCemPlus,
    // https://www.senecc.fi/projects/procemplus, various data from the Kampusareena building at Hervanta campus was gathered.
    // In addition, data from several other sources were gathered in the projects. The other data sources included, for example,
    // the electricity market prices and the weather measurements from a weather station located at the Sähkötalo building at Hervanta.
    // The data gathering system developed in the projects is still running and gathering data.
    //
    // A later, still ongoing, research project DELI, https://research.tuni.fi/tase/projects/, has as part of its agenda
    // to research the best ways to manage and share the collected data. In the project some of the ProCem data was uploaded
    // into a Apache IoTDB, https://iotdb.apache.org/, instance to test how well it could be used with the data.
    // IoTDB is a data management system for time series data. Some of the data uploaded to IoTDB is used in tasks 6-7.
    //
    // The IoTDB has a Spark connector plugin that can be used to load data from IoTDB directly into Spark DataFrame.
    // However, to not make things too complicated for the exercise, a ready-made sample of the data has already been
    // extracted and given as a static data for this and the following two tasks.
    //
    // The data
    //
    // The folder `data/ex3/procem` contains some ProCem data fetched from IoTDB in Parquet format.
    // The data includes one week from May 2024.
    //
    // Brief explanations on the columns:
    // - `Time`: the UNIX timestamp in millisecond precision
    // - `SolarPower`: the total electricity power produced by the solar panels on Kampusareena (W)
    // - `WaterCooling01Power` and `WaterCooling02Power`: the total electricity power used by the two water cooling machineries on Kampusareena (W)
    // - `VentilationPower`: the total electricity power used by the ventilation machinery on Kampusareena (W)
    // - `Temperature`: the temperature measured by the weather station on top of Sähkötalo (°C)
    // - `WindSpeed`: the wind speed measured by the weather station on top of Sähkötalo (m/s)
    // - `Humidity`: the humidity measured by the weather station on top of Sähkötalo (%)
    // - `ElectricityPrice`: the market price for electricity in Finland (€/MWh)
    //
    // Background information related to calculating hourly energies from the data:
    // - To get the hourly energy from the power: hourly_energy (kWh) = average_power_for_the_hour (W) / 1000
    // - The market price for electricity in Finland (during 2024) is given separately for each hour
    //   and does not change within the hour. Thus, there should be only one value for the price in each hour.
    //
    // The time in the ProCem data is given as UNIX timestamps in millisecond precision,
    // i.e., how many milliseconds has passed since January 1, 1970.<br>
    // 1716152400000 corresponds to "Monday, May 20, 2024 00:00:00.000" in UTC+0300 timezone.
    // Spark offers functions to do the conversion from the timestamps to a more human-readable format.
    //
    // The data contains a lot of NULL values. These NULL values mean that there is no measurement for that particular timestamp.
    // Both the power and weather measurements are given roughly in one second intervals.
    // Some measurements could be missing from the data, but those are not relevant for this exercise.
    //
    // The task
    //
    // Part 1:
    // - Read the data into `procemDF` DataFrame.
    //
    // Part 2:
    // - calculate the electrical energy produced by the solar panels for each hour (in `kWh`)
    // - calculate the total combined electrical energy consumed by the water cooling and ventilation machinery for each hour (in `kWh`)
    // - determine the price of the electrical energy for each hour (in `€/MWh`)
    // - calculate the average temperature for each hour (in `°C`)
    // - Give the result as a DataFrame, `hourlyDF`, where each row contains the hour and the corresponding four values.
    // - Order the DataFrame by the hour with the earliest hour first.
    //
    // In the example output, the datetime representation for the hour is given in UTC+0300 timezone
    // which was used in Finland (`Europe/Helsinki`) during May 2024.

    val dataPath: String = "../../data/ex3/procem"
    val procemDF: DataFrame = spark
        .read
        .parquet(dataPath)

    // handling of the timestamps is done carefully in 3 separate steps to make certain that
    // the Europe/Helsinki time zone (+0300 for May 2024) is used, regardless of the local time zone
    val hourlyDF: DataFrame = procemDF
        .withColumn("Time", timestamp_millis(col("Time")))
        .withColumn("Time", convert_timezone(current_timezone(), lit("Europe/Helsinki"), col("Time")))
        .withColumn("Time", date_trunc("hour", col("Time")))  // leave the minute and second parts off from the timestamps
        .groupBy("Time")
        .agg(
            avg(col("SolarPower")).as("AvgSolarPower"),
            avg(col("WaterCooling01Power")).as("AvgWaterCooling01Power"),
            avg(col("WaterCooling02Power")).as("AvgWaterCooling02Power"),
            avg(col("VentilationPower")).as("AvgVentilationPower"),
            avg(col("Temperature")).as("AvgTemperature"),
            any_value(col("ElectricityPrice")).as("ElectricityPrice")
        )
        .select(
            col("Time"),
            col("AvgTemperature"),
            (col("AvgSolarPower") / 1000).as("ProducedEnergy"),
            ((col("AvgWaterCooling01Power") + col("AvgWaterCooling02Power") + col("AvgVentilationPower")) / 1000).as("ConsumedEnergy"),
            col("ElectricityPrice").as("Price")
        )
        .orderBy(col("Time").asc)


    procemDF.printSchema()
    procemDF.limit(6).show()

    hourlyDF.printSchema()
    hourlyDF.limit(8).show(false)


    // Example output:
    // ===============
    // root
    //  |-- Time: long (nullable = true)
    //  |-- SolarPower: double (nullable = true)
    //  |-- WaterCooling01Power: double (nullable = true)
    //  |-- WaterCooling02Power: double (nullable = true)
    //  |-- VentilationPower: double (nullable = true)
    //  |-- Temperature: double (nullable = true)
    //  |-- WindSpeed: double (nullable = true)
    //  |-- Humidity: double (nullable = true)
    //  |-- ElectricityPrice: double (nullable = true)
    // +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+
    // |         Time|SolarPower|WaterCooling01Power|WaterCooling02Power|VentilationPower|Temperature|WindSpeed|Humidity|ElectricityPrice|
    // +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+
    // |1716152400000|      NULL|               NULL|               NULL|            NULL|       NULL|     NULL|    NULL|           -0.31|
    // |1716152400168|      NULL|               NULL|               NULL|            NULL|    14.0357|  4.32466| 53.0894|            NULL|
    // |1716152400217|      NULL|               NULL|               NULL|    24744.613281|       NULL|     NULL|    NULL|            NULL|
    // |1716152400277|      NULL|        4370.453613|               NULL|            NULL|       NULL|     NULL|    NULL|            NULL|
    // |1716152400605|      NULL|               NULL|          49.490608|            NULL|       NULL|     NULL|    NULL|            NULL|
    // |1716152400906| -6.500515|               NULL|               NULL|            NULL|       NULL|     NULL|    NULL|            NULL|
    // +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+
    // root
    //  |-- Time: timestamp (nullable = true)
    //  |-- AvgTemperature: double (nullable = true)
    //  |-- ProducedEnergy: double (nullable = true)
    //  |-- ConsumedEnergy: double (nullable = true)
    //  |-- Price: double (nullable = true)
    // +-------------------+------------------+---------------------+------------------+-----+
    // |Time               |AvgTemperature    |ProducedEnergy       |ConsumedEnergy    |Price|
    // +-------------------+------------------+---------------------+------------------+-----+
    // |2024-05-20 00:00:00|13.754773048068902|-0.00583301298888889 |38.668148279083816|-0.31|
    // |2024-05-20 01:00:00|13.209065638888916|-0.005838116895833327|36.66061308699361 |-0.3 |
    // |2024-05-20 02:00:00|11.78702305555553 |-0.005843125166944462|37.91117283675028 |-0.1 |
    // |2024-05-20 03:00:00|10.510957036111114|-0.005829664534999985|35.09305738077221 |-0.03|
    // |2024-05-20 04:00:00|8.989454824999994 |0.0773709636641667   |36.59321714971756 |0.01 |
    // |2024-05-20 05:00:00|8.072131227777806 |0.8554100720902555   |33.8054992999987  |1.41 |
    // |2024-05-20 06:00:00|8.412301513888893 |2.3616457910869446   |54.32778184731544 |4.94 |
    // |2024-05-20 07:00:00|9.190588663888901 |11.20785037204834    |53.04797348656393 |10.44|
    // +-------------------+------------------+---------------------+------------------+-----+



    printTaskLine(7)
    // Task 7 - Daily electricity costs
    //
    // Background information:
    // - The energy that is considered to be bought from the electricity market is the difference between the consumed and produced energy.
    //
    // To get the hourly cost for the energy bought from the market:
    //   hourly_cost (€) = hourly_energy_from_market (kWh) * electricity_price_for_hour (€/MWh) / 1000
    //
    // Note, that any consumer buying electricity would also have to pay additional fees (taxes, transfer fees, etc.)
    // that are not considered in this exercise.
    // And that the given power consumption is only a part of the overall power consumption at Kampusareena.
    //
    // Using `hourlyDF` DataFrame from task 6 as a starting point:
    // - calculate the average daily temperatures (in `°C`)
    // - calculate the total daily energy produced by the solar panels (in `kWh`)
    // - calculate the total daily energy consumed by the water cooling and ventilation machinery (in `kWh`)
    // - calculate the total daily price for the energy that was bought from the electricity market (in `€`)
    //
    // Give the result as a DataFrame where each row contains the date and the corresponding four values rounded to two decimals.
    // Order the DataFrame by the date in chronological order.
    //
    // Finally, calculate the total electricity price for the entire week.

    val dailyDF: DataFrame = hourlyDF
        .withColumn(
            "HourlyCost",
            (col("ConsumedEnergy") - col("ProducedEnergy")) * col("Price") / 1000
        )
        .withColumn("Date", to_date(col("Time")))
        .groupBy("Date")
        .agg(
            round(avg("AvgTemperature"), 2).as("AvgTemperature"),
            round(sum("ProducedEnergy"), 2).as("ProducedEnergy"),
            round(sum("ConsumedEnergy"), 2).as("ConsumedEnergy"),
            round(sum("HourlyCost"), 2).as("DailyCost")
        )
        .orderBy(col("Date").asc)


    val totalPrice: Double = dailyDF
        .select(sum(col("DailyCost")))
        .head()
        .getDouble(0)


    dailyDF.show()

    println(s"Total price: ${totalPrice} €")


    // Example output:
    // ===============
    // +----------+-----------+--------------+--------------+---------+
    // |      Date|Temperature|ProducedEnergy|ConsumedEnergy|DailyCost|
    // +----------+-----------+--------------+--------------+---------+
    // |2024-05-20|      13.02|         373.9|       1084.76|     9.11|
    // |2024-05-21|      12.91|         369.7|        1154.5|    16.84|
    // |2024-05-22|      17.75|        355.15|       1708.37|    10.63|
    // |2024-05-23|      19.79|        360.76|       1948.03|      5.5|
    // |2024-05-24|      19.68|        258.41|       1978.22|    35.53|
    // |2024-05-25|      20.79|        294.36|       1533.66|     10.8|
    // |2024-05-26|       19.9|        265.03|       1264.05|     3.36|
    // +----------+-----------+--------------+--------------+---------+
    // Total price: 91.77 €



    printTaskLine(8)
    // Task 8 - Theory question
    //
    // Using your own words, answer the following questions:
    //
    // 1. In this and the previous exercise, you have been using Spark to do the exercise tasks.<br>
    //    What is Spark, and why is it used in the course?
    // 2. In general, when can using Spark be beneficial?
    //    (compared to, for example, handling the data processing with native programming language tools, or other libraries like Pandas)
    // 3. Are there drawbacks related to using Spark?
    //
    // Extensive answers are not required here.
    // If your answers do not fit into one screen, you have likely written more than what was expected.

    // 1. The course is called Data-Intensive Programming, and one of the goals is to teach tools and techniques that
    //    can be used to deal with large amounts of data. Spark is a distributed data processing framework that is well
    //    suited for processing large datasets. It can be used locally with a single computer (allowing parallelism, i.e.,
    //    the use of multiple CPU cores for processing datasets), but allows distributing the processing efficiently across
    //    multiple computers. It is also currently widely used in the industry.
    // 2. Using Spark is especially beneficial when working with large datasets that do not fit into the memory or the
    //    disk space of a single computer (which might be difficult to handle with, for example, Pandas). And when dealing
    //    with slightly smaller datasets, it provides multithreading to the data processing by default, which might not be
    //    simple to do with native programming tools, but can speed up the data processing.
    // 3. For small datasets, using Spark can introduce increased complexity and harder debugging compared to some simpler
    //    tools. Also, with small datasets, the performance can be slower compared to tools like Pandas due to overhead in
    //    starting and distributing the calculations to the cluster.



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
