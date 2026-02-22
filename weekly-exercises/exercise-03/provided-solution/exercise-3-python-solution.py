# Databricks notebook source
# MAGIC %md
# MAGIC Copyright 2025 Tampere University<br>
# MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
# MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
# MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

# COMMAND ----------

# MAGIC %md
# MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 3
# MAGIC
# MAGIC This exercise contains additional data processing tasks using DataFrames. The last task is a theory question related to the lectures.<br>
# MAGIC The coding tasks can be slightly more challenging than those in exercise 2.
# MAGIC
# MAGIC This is the **Python** version, switch to the Scala version if you want to do the tasks in Scala.
# MAGIC
# MAGIC Each task has its own cell(s) for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary.<br>
# MAGIC There are cells with test code and example output following most of the tasks that involve producing code.
# MAGIC
# MAGIC At the end of the notebook, there is a question regarding the use of AI or other collaboration when working the tasks.<br>
# MAGIC Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle: [Weekly Exercise #3](https://moodle.tuni.fi/mod/assign/view.php?id=3503818)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Some resources that can help with the tasks in this exercise:
# MAGIC
# MAGIC - The [tutorial notebook](https://adb-7895492183558578.18.azuredatabricks.net/editor/notebooks/743402606902162) from our course
# MAGIC - Chapters 3 and 5 (Section: Common DataFrames and Spark SQL Operations) in [Learning Spark, 2nd Edition](https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
# MAGIC     - There are additional code examples in the related [GitHub repository](https://github.com/databricks/LearningSparkV2).
# MAGIC     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL<br> `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
# MAGIC - [Databricks tutorial](https://docs.databricks.com/en/getting-started/dataframes.html) of using Spark DataFrames
# MAGIC - [Apache Spark documentation](https://spark.apache.org/docs/3.5.6/sql-ref-functions.html) on all available functions that can be used on DataFrames.<br>
# MAGIC   The full [Spark Python functions API listing](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/functions.html) for the functions package might have some additional functions listed that have not been updated in the documentation.

# COMMAND ----------

# some imports that might be required in the tasks
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Load new data to DataFrames
# MAGIC
# MAGIC The data used in tasks 1-5 is weather data that is based on a dataset from Kaggle: [The Weather Dataset](https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data).<br>
# MAGIC
# MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) in the `exercises/ex3/weather` folder, there are two subfolders: `measurements` and `metadata`.
# MAGIC
# MAGIC - `measurements` folder contains daily weather data in Parquet format in file `daily_weather.parquet`.
# MAGIC - `metadata` folder contains two separate CSV files, `cities.csv` and `countries.csv`
# MAGIC
# MAGIC Some columns and rows have been removed for this task. For the remaining columns, the Kaggle page has further descriptions on the data.
# MAGIC
# MAGIC Parquet is a column-oriented data file format designed for efficient data storage and retrieval. Unlike CSV files, the data given in Parquet format is not as easy to preview without Spark. But if you really want, for example, the [Parquet Visualizer](https://marketplace.visualstudio.com/items?itemName=lucien-martijn.parquet-visualizer) Visual Studio Code extension can be used to browse the data contained in a Parquet file. However, understanding the format is not important for this exercise. The format will be discussed in more detail during the lectures later in the course.
# MAGIC
# MAGIC ##### The task
# MAGIC
# MAGIC - Load the daily weather data into DataFrame `weatherDF`.
# MAGIC - Load the city related metadata into DataFrame `cityDF`.
# MAGIC - Load the country related metadata into DataFrame `countryDF`.

# COMMAND ----------

basePath: str = "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex3/weather"
measurementPath: str = f"{basePath}/measurements"
cityFilePath: str = f"{basePath}/metadata/cities.csv"
countryFilePath: str = f"{basePath}/metadata/countries.csv"

# if the given path is a folder, Spark reads all the files from the folder
weatherDF: DataFrame = spark \
    .read \
    .parquet(measurementPath)

cityDF: DataFrame = spark \
    .read \
    .option("header", "true") \
    .option("sep", ",") \
    .option("inferSchema", "true") \
    .csv(cityFilePath)

countryDF: DataFrame = spark \
    .read \
    .option("header", "true") \
    .option("sep", ",") \
    .option("inferSchema", "true") \
    .csv(countryFilePath)

# alternative version providing explicit schema for the CSV data to avoid an extra passthrough of the CSV file to infer the schema
citySchema: StructType = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("city_name", StringType(), True),
        StructField("iso2", StringType(), True),
        StructField("iso3", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]
)
countrySchema: StructType = StructType(
    [
        StructField("country", StringType(), True),
        StructField("native_name", StringType(), True),
        StructField("iso2", StringType(), True),
        StructField("iso3", StringType(), True),
        StructField("population", DoubleType(), True),
        StructField("area", DoubleType(), True),
        StructField("capital", StringType(), True),
        StructField("region", StringType(), True),
        StructField("continent", StringType(), True)
    ]
)
cityDF2: DataFrame = spark \
    .read \
    .option("header", "true") \
    .option("sep", ",") \
    .schema(citySchema) \
    .csv(cityFilePath)
countryDF2: DataFrame = spark \
    .read \
    .option("header", "true") \
    .option("sep", ",") \
    .schema(countrySchema) \
    .csv(countryFilePath)

# COMMAND ----------

print("The first 5 rows of the weather data")
display(weatherDF.limit(5))

# COMMAND ----------

print("The first 5 rows of the city data")
display(cityDF.limit(5))

# COMMAND ----------

print("The first 5 rows of the country data")
display(countryDF.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC The first 5 rows of the weather data
# MAGIC +----------+----------+----------+----------+----------------+
# MAGIC |station_id|      date|min_temp_c|max_temp_c|precipitation_mm|
# MAGIC +----------+----------+----------+----------+----------------+
# MAGIC |     01008|2000-01-01|      -6.7|      -0.8|             1.0|
# MAGIC |     01026|2000-01-01|      -6.1|      -1.6|             0.0|
# MAGIC |     01271|2000-01-01|      -5.1|       4.4|             2.0|
# MAGIC |     01465|2000-01-01|       1.8|       6.5|             0.6|
# MAGIC |     01492|2000-01-01|      -8.6|      -3.2|             6.0|
# MAGIC +----------+----------+----------+----------+----------------+
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC The first 5 rows of the city data
# MAGIC +----------+----------+----+----+-------------+-------------+
# MAGIC |station_id| city_name|iso2|iso3|     latitude|    longitude|
# MAGIC +----------+----------+----+----+-------------+-------------+
# MAGIC |     41515|  Asadabad|  AF| AFG|34.8660000397|71.1500045859|
# MAGIC |     38954|  Fayzabad|  AF| AFG|37.1297607616|70.5792471913|
# MAGIC |     41560| Jalalabad|  AF| AFG|34.4415269155|70.4361034738|
# MAGIC |     38947|    Kunduz|  AF| AFG|36.7279506623|68.8725296619|
# MAGIC |     38987|Qala i Naw|  AF| AFG| 34.983000131|63.1332996367|
# MAGIC +----------+----------+----+----+-------------+-------------+
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC The first 5 rows of the country data
# MAGIC +--------------+--------------+----+----+----------+---------+----------------+--------------------+---------+
# MAGIC |       country|   native_name|iso2|iso3|population|     area|         capital|              region|continent|
# MAGIC +--------------+--------------+----+----+----------+---------+----------------+--------------------+---------+
# MAGIC |   Afghanistan|         افغانستان|  AF| AFG| 2.60231E7| 652230.0|           Kabul|Southern and Cent...|     Asia|
# MAGIC |       Albania|     Shqipëria|  AL| ALB| 2895947.0|  28748.0|          Tirana|     Southern Europe|   Europe|
# MAGIC |       Algeria|          الجزائر|  DZ| DZA|    3.87E7|2381741.0|         Algiers|     Northern Africa|   Africa|
# MAGIC |American Samoa|American Samoa|  AS| ASM|   55519.0|    199.0|       Pago Pago|           Polynesia|  Oceania|
# MAGIC |       Andorra|          NULL|  AD| AND|   87486.0|   4678.0|Andorra la Vella|     Southern Europe|   Europe|
# MAGIC +--------------+--------------+----+----+----------+---------+----------------+--------------------+---------+
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Combine data into a single DataFrame
# MAGIC
# MAGIC Part 1:
# MAGIC
# MAGIC - Create a new `metadataDF` DataFrame where the country information from `countryDF` has been added to `cityDF`.
# MAGIC - The new DataFrame should have the same number of rows as the original `cityDF`.
# MAGIC - Hint: either `iso2` or `iso3` column can be used to connect the city and country data.
# MAGIC
# MAGIC Part 2:
# MAGIC
# MAGIC - Create a new `fullDF` DataFrame where the metadata from `metadataDF` has been added to the weather measurements `weatherDF`.
# MAGIC - The new DataFrame should have the same number of rows as the original `weatherDF`.
# MAGIC
# MAGIC In the example outputs, all columns that will not be needed in the following tasks have been removed. Removing the columns here is allowed, but not compulsory.

# COMMAND ----------

metadataDF: DataFrame = cityDF \
    .drop("iso2", "longitude") \
    .join(
        countryDF
            .drop("iso2", "native_name", "population", "area", "region", "continent"),
        ["iso3"],
        "inner"
    ) \
    .drop("iso3")

fullDF: DataFrame = weatherDF \
    .join(metadataDF, ["station_id"], "inner")

# COMMAND ----------

metadataDF.limit(8).show(8, False)
print(f"Number of rows in the metadata DataFrame: {metadataDF.count()}")

# COMMAND ----------

fullDF.limit(8).show(8, False)
print(f"Number of rows in the full DataFrame: {fullDF.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC (depending how the data combination is done, the first data rows might not be the same as shown here)<br>
# MAGIC (in these example outputs, columns that will not be needed in later tasks have been removed)
# MAGIC ```text
# MAGIC +----------+----------+-------------+-----------+-------+
# MAGIC |station_id|city_name |latitude     |country    |capital|
# MAGIC +----------+----------+-------------+-----------+-------+
# MAGIC |41515     |Asadabad  |34.8660000397|Afghanistan|Kabul  |
# MAGIC |38954     |Fayzabad  |37.1297607616|Afghanistan|Kabul  |
# MAGIC |41560     |Jalalabad |34.4415269155|Afghanistan|Kabul  |
# MAGIC |38947     |Kunduz    |36.7279506623|Afghanistan|Kabul  |
# MAGIC |38987     |Qala i Naw|34.983000131 |Afghanistan|Kabul  |
# MAGIC |38915     |Sheberghan|36.6579807729|Afghanistan|Kabul  |
# MAGIC |13577     |Peshkopi  |41.6833020982|Albania    |Tirana |
# MAGIC |13461     |Shkodër   |42.0684515575|Albania    |Tirana |
# MAGIC +----------+----------+-------------+-----------+-------+
# MAGIC
# MAGIC Number of rows in the metadata DataFrame: 1208
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC +----------+----------+----------+----------+----------------+------------+-------------+----------------------+------------+
# MAGIC |station_id|date      |min_temp_c|max_temp_c|precipitation_mm|city_name   |latitude     |country               |capital     |
# MAGIC +----------+----------+----------+----------+----------------+------------+-------------+----------------------+------------+
# MAGIC |01008     |2000-01-01|-6.7      |-0.8      |1.0             |Longyearbyen|78.2166843864|Svalbard and Jan Mayen|Longyearbyen|
# MAGIC |01026     |2000-01-01|-6.1      |-1.6      |0.0             |Tromsø      |69.635076227 |Norway                |Oslo        |
# MAGIC |01271     |2000-01-01|-5.1      |4.4       |2.0             |Trondheim   |63.4166575309|Norway                |Oslo        |
# MAGIC |01465     |2000-01-01|1.8       |6.5       |0.6             |Arendal     |58.4647560555|Norway                |Oslo        |
# MAGIC |01492     |2000-01-01|-8.6      |-3.2      |6.0             |Oslo        |59.9166902864|Norway                |Oslo        |
# MAGIC |02433     |2000-01-01|-16.6     |-6.2      |6.2             |Falun       |60.6130020356|Sweden                |Stockholm   |
# MAGIC |02485     |2000-01-01|-9.3      |0.7       |1.7             |Stockholm   |59.3507599543|Sweden                |Stockholm   |
# MAGIC |02562     |2000-01-01|-5.1      |1.1       |0.7             |Linköping   |58.4100122265|Sweden                |Stockholm   |
# MAGIC +----------+----------+----------+----------+----------------+------------+-------------+----------------------+------------+
# MAGIC
# MAGIC Number of rows in the full DataFrame: 5130470
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - The coldest and the hottest day in each country
# MAGIC
# MAGIC Part 1:
# MAGIC
# MAGIC - Find the lowest temperature and the date it happened for each country.
# MAGIC - Order the countries such that the country with coldest temperature is given first.
# MAGIC - Store the result in `coldDayDF` DataFrame.
# MAGIC
# MAGIC Part 2:
# MAGIC
# MAGIC - Find the highest temperature and the date it happened for each country.
# MAGIC - Order the countries such that the country with hottest temperature is given first.
# MAGIC - Store the result in `hotDayDF` DataFrame.

# COMMAND ----------

extremeDayDF: DataFrame = fullDF \
    .groupBy("country") \
    .agg(
        F.min("min_temp_c").alias("coldest_temp"),
        F.min_by(F.col("date"), F.col("min_temp_c")).alias("coldest_date"),
        F.max("max_temp_c").alias("hottest_temp"),
        F.max_by(F.col("date"), F.col("max_temp_c")).alias("hottest_date")
    )

coldDayDF: DataFrame = extremeDayDF \
    .select(
        F.col("country"),
        F.col("coldest_temp"),
        F.col("coldest_date").alias("date")
    ) \
    .orderBy(F.col("coldest_temp").asc(), F.col("date").asc())

hotDayDF: DataFrame = extremeDayDF \
    .select(
        F.col("country"),
        F.col("hottest_temp"),
        F.col("hottest_date").alias("date")
    ) \
    .orderBy(F.col("hottest_temp").desc(), F.col("date").asc())

# COMMAND ----------

coldDayDF.limit(10).show()

# COMMAND ----------

hotDayDF.limit(10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC +-------------+------------+----------+
# MAGIC |      country|coldest_temp|      date|
# MAGIC +-------------+------------+----------+
# MAGIC |       Russia|       -55.4|2000-01-07|
# MAGIC |       Canada|       -46.6|2005-01-13|
# MAGIC |     Mongolia|       -46.6|2005-02-09|
# MAGIC |      Tunisia|       -45.1|2023-04-01|
# MAGIC |     Dominica|       -44.9|2005-01-17|
# MAGIC |   Kazakhstan|       -42.6|2001-01-07|
# MAGIC |United States|       -42.2|2009-01-15|
# MAGIC |       Latvia|       -41.3|2020-01-17|
# MAGIC |   Kyrgyzstan|       -39.7|2023-01-13|
# MAGIC |      Finland|       -39.4|2010-02-20|
# MAGIC +-------------+------------+----------+
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC +----------+------------+----------+
# MAGIC |   country|hottest_temp|      date|
# MAGIC +----------+------------+----------+
# MAGIC |     Chile|        90.0|2020-04-12|
# MAGIC |    Brazil|        80.0|2021-03-17|
# MAGIC |  Tanzania|        78.0|2020-12-19|
# MAGIC |     Ghana|        60.5|2023-03-13|
# MAGIC | Argentina|        60.0|2018-11-09|
# MAGIC |   Lesotho|        58.0|2023-02-13|
# MAGIC |      Laos|        56.0|2019-05-17|
# MAGIC |Mozambique|        56.0|2022-05-14|
# MAGIC |  Colombia|        56.0|2023-04-17|
# MAGIC |     India|        56.0|2023-06-05|
# MAGIC +----------+------------+----------+
# MAGIC ```
# MAGIC
# MAGIC Note that some results are not believable and imply partly broken data.<br>
# MAGIC For example, Dominica (an island in Caribbean) should not get negative temperatures. And the first few hottest temperatures are too high to be believable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Extreme days in Nordic countries
# MAGIC
# MAGIC In this task only the weather data for the independent Nordic countries (`Finland`, `Sweden`, `Norway`, `Denmark`, `Iceland`) should be considered.
# MAGIC
# MAGIC Part 1:
# MAGIC
# MAGIC - Similarly to task 3, find the coldest and the hottest days (including the dates) for the five Nordic countries.
# MAGIC - Also, find the date with the most rain (including the rain amount) for the five Nordic countries.
# MAGIC - Store the result in `nordicExtremeDayDF` DataFrame.
# MAGIC
# MAGIC Part 2:
# MAGIC
# MAGIC - Repeat part 1, but only consider data that is measured in the capital city of each Nordic country.
# MAGIC - Store the result in `nordicCapitalExtremeDayDF` DataFrame.

# COMMAND ----------

nordicDF: DataFrame = fullDF \
    .filter(F.col("country").isin("Finland", "Sweden", "Norway", "Denmark", "Iceland"))

nordicCapitalDF: DataFrame = nordicDF \
    .filter(F.col("city_name") == F.col("capital"))

# helper function to avoid repeating code
def getExtremeDF(inputDF: DataFrame) -> DataFrame:
    return inputDF \
        .groupBy("country") \
        .agg(
            F.min("min_temp_c").alias("coldest_temp"),
            F.min_by(F.col("date"), F.col("min_temp_c")).alias("coldest_date"),
            F.max("max_temp_c").alias("hottest_temp"),
            F.max_by(F.col("date"), F.col("max_temp_c")).alias("hottest_date"),
            F.max("precipitation_mm").alias("max_rainfall"),
            F.max_by(F.col("date"), F.col("precipitation_mm")).alias("rainiest_date")
        )

nordicExtremeDayDF: DataFrame = getExtremeDF(nordicDF)

nordicCapitalExtremeDF: DataFrame = getExtremeDF(nordicCapitalDF)

# COMMAND ----------

print("Extreme days in Nordic countries including measurements from all stations:")
nordicExtremeDayDF.show()

# COMMAND ----------

print("Extreme days in Nordic countries using only measurements from country capitals:")
nordicCapitalExtremeDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC Extreme days in Nordic countries including measurements from all stations:
# MAGIC +-------+------------+------------+------------+------------+------------+-------------+
# MAGIC |country|coldest_temp|coldest_date|hottest_temp|hottest_date|max_rainfall|rainiest_date|
# MAGIC +-------+------------+------------+------------+------------+------------+-------------+
# MAGIC | Sweden|       -28.9|  2010-01-30|        36.9|  2022-07-21|       102.1|   2022-08-27|
# MAGIC |Finland|       -39.4|  2010-02-20|        35.0|  2010-07-29|        86.1|   2004-07-28|
# MAGIC | Norway|       -25.1|  2010-02-23|        34.6|  2018-07-27|        90.9|   2022-10-24|
# MAGIC |Denmark|       -18.6|  2010-12-21|        36.7|  2022-07-20|       110.0|   2005-12-06|
# MAGIC |Iceland|       -15.2|  2022-12-31|        25.7|  2008-07-30|        66.8|   2001-10-31|
# MAGIC +-------+------------+------------+------------+------------+------------+-------------+
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC Extreme days in Nordic countries using only measurements from country capitals:
# MAGIC +-------+------------+------------+------------+------------+------------+-------------+
# MAGIC |country|coldest_temp|coldest_date|hottest_temp|hottest_date|max_rainfall|rainiest_date|
# MAGIC +-------+------------+------------+------------+------------+------------+-------------+
# MAGIC | Sweden|       -21.0|  2010-02-22|        34.5|  2022-07-21|        44.0|   2014-09-21|
# MAGIC |Finland|       -21.2|  2021-01-15|        30.2|  2021-07-15|        62.0|   2019-08-23|
# MAGIC | Norway|       -20.7|  2001-02-05|        34.6|  2018-07-27|        72.8|   2014-06-26|
# MAGIC |Iceland|       -15.2|  2022-12-31|        25.7|  2008-07-30|        54.6|   2012-12-29|
# MAGIC +-------+------------+------------+------------+------------+------------+-------------+
# MAGIC ```
# MAGIC
# MAGIC There are no measurements from Copenhagen, the capital of Denmark, which is why Denmark is not included in the second output.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Countries with dry months
# MAGIC
# MAGIC For this task, a country is considered to have a "fully dry month" only if
# MAGIC
# MAGIC - there are measurements from the capital of the country for each day of the month
# MAGIC - and none of the measurements from the capital show rain, i.e., the precipitation is not larger than 0
# MAGIC
# MAGIC Note that there is only one row for each (station_id, date)-pair in the weather data. All duplicates in the original data have been removed for this exercise's dataset.
# MAGIC
# MAGIC **Part 1:**
# MAGIC
# MAGIC - Find how many fully dry months each country had in the year 2022.
# MAGIC - Store the country name and the dry-month count for those countries that had at least 3 dry months into DataFrame `threeDryMonthsDF`.
# MAGIC
# MAGIC **Part 2:**
# MAGIC
# MAGIC - Find the names of the countries for which the capital is in the Southern Hemisphere (i.e, latitude < 0), and that had at least one fully dry month in the year 2022.
# MAGIC - Store the names as a list of strings to `southernDryCountries`.

# COMMAND ----------

dryMonthsDF: DataFrame = fullDF \
    .filter(
        (F.col("city_name") == F.col("capital")) & (F.year(F.col("date")) == 2022)
    ) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("days_in_month", F.dayofmonth(F.last_day(F.col("date")))) \
    .groupBy("country", "month") \
    .agg(
        F.first("days_in_month").alias("days_in_month"),
        F.count(F.when(F.col("precipitation_mm") <= 0, True)).alias("dry_day_count"),
        F.first(F.col("latitude")).alias("latitude")
    ) \
    .filter(F.col("dry_day_count") == F.col("days_in_month")) \
    .groupBy("country") \
    .agg(
        F.count("*").alias("dry_month_count"),
        F.first(F.col("latitude")).alias("latitude")
    )

threeDryMonthsDF: DataFrame = dryMonthsDF \
    .filter(F.col("dry_month_count") >= 3) \
    .drop("latitude") \
    .orderBy(F.col("dry_month_count").desc(), F.col("country").asc())

southernDryCountryRows: list[Row] = dryMonthsDF \
    .filter((F.col("dry_month_count") >= 1) & (F.col("latitude") < 0)) \
    .select("country") \
    .orderBy(F.col("country").asc()) \
    .collect()
southernDryCountries: list[str] = [row[0] for row in southernDryCountryRows]

# COMMAND ----------

threeDryMonthsDF.show()

# COMMAND ----------

print("The southern hemisphere countries with at least one fully dry month in 2022:")
for country in southernDryCountries:
    print(f"- {country}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC +--------------------+---------------+
# MAGIC |             country|dry_month_count|
# MAGIC +--------------------+---------------+
# MAGIC |                Oman|              5|
# MAGIC |             Bahrain|              4|
# MAGIC |               Qatar|              4|
# MAGIC |        Saudi Arabia|              4|
# MAGIC |               Syria|              4|
# MAGIC |United Arab Emirates|              4|
# MAGIC |                Iran|              3|
# MAGIC |             Namibia|              3|
# MAGIC |          Tajikistan|              3|
# MAGIC |            Tanzania|              3|
# MAGIC +--------------------+---------------+
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC The southern hemisphere countries with at least one fully dry month in 2022:
# MAGIC - Angola
# MAGIC - Namibia
# MAGIC - Tanzania
# MAGIC - Zimbabwe
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Electricity data and hourly averages
# MAGIC
# MAGIC ##### Background information related to the data source
# MAGIC
# MAGIC As part of Tampere University research projects [ProCem](https://www.senecc.fi/projects/procem-2) and [ProCemPlus](https://www.senecc.fi/projects/procemplus) various data from the Kampusareena building at Hervanta campus was gathered. In addition, data from several other sources were gathered in the projects. The other data sources included, for example, the electricity market prices and the weather measurements from a weather station located at the Sähkötalo building at Hervanta. The data gathering system developed in the projects is still running and gathering data.
# MAGIC
# MAGIC A later, still ongoing, research project [DELI](https://research.tuni.fi/tase/projects/) has as part of its agenda to research the best ways to manage and share the collected data. In the project some of the ProCem data was uploaded into a [Apache IoTDB](https://iotdb.apache.org/) instance to test how well it could be used with the data. IoTDB is a data management system for time series data. Some of the data uploaded to IoTDB is used in tasks 6-7.
# MAGIC
# MAGIC The IoTDB has a Spark connector plugin that can be used to load data from IoTDB directly into Spark DataFrame. However, to not make things too complicated for the exercise, a ready-made sample of the data has already been extracted and given as a static data for this and the following two tasks.
# MAGIC
# MAGIC ##### The data
# MAGIC
# MAGIC The [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) folder `exercises/ex3/procem` contains some ProCem data fetched from IoTDB in Parquet format. The data includes one week from May 2024.
# MAGIC
# MAGIC Brief explanations on the columns:
# MAGIC
# MAGIC - `Time`: the UNIX timestamp in millisecond precision
# MAGIC - `SolarPower`: the total electricity power produced by the solar panels on Kampusareena (`W`)
# MAGIC - `WaterCooling01Power` and `WaterCooling02Power`: the total electricity power used by the two water cooling machineries on Kampusareena (`W`)
# MAGIC - `VentilationPower`: the total electricity power used by the ventilation machinery on Kampusareena (`W`)
# MAGIC - `Temperature`: the temperature measured by the weather station on top of Sähkötalo (`°C`)
# MAGIC - `WindSpeed`: the wind speed measured by the weather station on top of Sähkötalo (`m/s`)
# MAGIC - `Humidity`: the humidity measured by the weather station on top of Sähkötalo (`%`)
# MAGIC - `ElectricityPrice`: the market price for electricity in Finland (`€/MWh`)
# MAGIC
# MAGIC ##### Background information related to calculating hourly energies from the data:
# MAGIC
# MAGIC To get the hourly energy from the power: `hourly_energy (kWh) = average_power_for_the_hour (W) / 1000`
# MAGIC
# MAGIC The market price for electricity in Finland (during 2024) is given separately for each hour and does not change within the hour. Thus, there should be only one value for the price in each hour.
# MAGIC
# MAGIC The time in the ProCem data is given as UNIX timestamps in millisecond precision, i.e., how many milliseconds has passed since January 1, 1970.<br>
# MAGIC `1716152400000` corresponds to `Monday, May 20, 2024 00:00:00.000` in UTC+0300 timezone. Spark offers functions to do the conversion from the timestamps to a more human-readable format.
# MAGIC
# MAGIC The data contains a lot of NULL values. These NULL values mean that there is no measurement for that particular timestamp. Both the power and weather measurements are given roughly in one second intervals. Some measurements could be missing from the data, but those are not relevant for this exercise.
# MAGIC
# MAGIC ##### The task
# MAGIC
# MAGIC Part 1
# MAGIC
# MAGIC - Read the data into `procemDF` DataFrame.
# MAGIC
# MAGIC Part 2
# MAGIC
# MAGIC - calculate the electrical energy produced by the solar panels for each hour (in `kWh`)
# MAGIC - calculate the total combined electrical energy consumed by the water cooling and ventilation machinery for each hour (in `kWh`)
# MAGIC - determine the price of the electrical energy for each hour (in `€/MWh`)
# MAGIC - calculate the average temperature for each hour (in `°C`)
# MAGIC - Give the result as a DataFrame, `hourlyDF`, where each row contains the hour and the corresponding four values.
# MAGIC - Order the DataFrame by the hour with the earliest hour first.
# MAGIC
# MAGIC In the example output, the datetime representation for the hour is given in UTC+0300 timezone which was used in Finland (`Europe/Helsinki`) during May 2024.

# COMMAND ----------

dataPath: str = "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/exercises/ex3/procem"
procemDF: DataFrame = spark \
    .read \
    .parquet(dataPath)

# COMMAND ----------

procemDF.printSchema()
procemDF.limit(6).show()

# COMMAND ----------

# handling of the timestamps is done carefully in 3 separate steps to make certain that the Europe/Helsinki time zone (+0300 for May 2024) is used, regardless of the local time zone
# the minute and second parts off from the timestamps by using date_trunc
hourlyDF: DataFrame = procemDF \
    .withColumn("Time", F.timestamp_millis(F.col("Time"))) \
    .withColumn(
        "Time",
        F.convert_timezone(F.current_timezone(), F.lit("Europe/Helsinki"), F.col("Time"))
    ) \
    .withColumn("Time", F.date_trunc("hour", F.col("Time"))) \
    .groupBy("Time") \
    .agg(
        F.avg(F.col("SolarPower")).alias("AvgSolarPower"),
        F.avg(F.col("WaterCooling01Power")).alias("AvgWaterCooling01Power"),
        F.avg(F.col("WaterCooling02Power")).alias("AvgWaterCooling02Power"),
        F.avg(F.col("VentilationPower")).alias("AvgVentilationPower"),
        F.avg(F.col("Temperature")).alias("AvgTemperature"),
        F.any_value(F.col("ElectricityPrice")).alias("ElectricityPrice")
    ) \
    .select(
        F.col("Time"),
        F.col("AvgTemperature"),
        (F.col("AvgSolarPower") / 1000).alias("ProducedEnergy"),
        ((F.col("AvgWaterCooling01Power") + F.col("AvgWaterCooling02Power") + F.col("AvgVentilationPower")) / 1000).alias("ConsumedEnergy"),
        F.col("ElectricityPrice").alias("Price")
    ) \
    .orderBy(F.col("Time").asc())

# COMMAND ----------

hourlyDF.printSchema()
hourlyDF.limit(8).show(8, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC root
# MAGIC  |-- Time: long (nullable = true)
# MAGIC  |-- SolarPower: double (nullable = true)
# MAGIC  |-- WaterCooling01Power: double (nullable = true)
# MAGIC  |-- WaterCooling02Power: double (nullable = true)
# MAGIC  |-- VentilationPower: double (nullable = true)
# MAGIC  |-- Temperature: double (nullable = true)
# MAGIC  |-- WindSpeed: double (nullable = true)
# MAGIC  |-- Humidity: double (nullable = true)
# MAGIC  |-- ElectricityPrice: double (nullable = true)
# MAGIC
# MAGIC +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+
# MAGIC |         Time|SolarPower|WaterCooling01Power|WaterCooling02Power|VentilationPower|Temperature|WindSpeed|Humidity|ElectricityPrice|
# MAGIC +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+
# MAGIC |1716152400000|      NULL|               NULL|               NULL|            NULL|       NULL|     NULL|    NULL|           -0.31|
# MAGIC |1716152400168|      NULL|               NULL|               NULL|            NULL|    14.0357|  4.32466| 53.0894|            NULL|
# MAGIC |1716152400217|      NULL|               NULL|               NULL|    24744.613281|       NULL|     NULL|    NULL|            NULL|
# MAGIC |1716152400277|      NULL|        4370.453613|               NULL|            NULL|       NULL|     NULL|    NULL|            NULL|
# MAGIC |1716152400605|      NULL|               NULL|          49.490608|            NULL|       NULL|     NULL|    NULL|            NULL|
# MAGIC |1716152400906| -6.500515|               NULL|               NULL|            NULL|       NULL|     NULL|    NULL|            NULL|
# MAGIC +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC root
# MAGIC  |-- Time: timestamp (nullable = true)
# MAGIC  |-- AvgTemperature: double (nullable = true)
# MAGIC  |-- ProducedEnergy: double (nullable = true)
# MAGIC  |-- ConsumedEnergy: double (nullable = true)
# MAGIC  |-- Price: double (nullable = true)
# MAGIC
# MAGIC +-------------------+------------------+---------------------+------------------+-----+
# MAGIC |Time               |AvgTemperature    |ProducedEnergy       |ConsumedEnergy    |Price|
# MAGIC +-------------------+------------------+---------------------+------------------+-----+
# MAGIC |2024-05-20 00:00:00|13.754773048068902|-0.00583301298888889 |38.668148279083816|-0.31|
# MAGIC |2024-05-20 01:00:00|13.209065638888916|-0.005838116895833327|36.66061308699361 |-0.3 |
# MAGIC |2024-05-20 02:00:00|11.78702305555553 |-0.005843125166944462|37.91117283675028 |-0.1 |
# MAGIC |2024-05-20 03:00:00|10.510957036111114|-0.005829664534999985|35.09305738077221 |-0.03|
# MAGIC |2024-05-20 04:00:00|8.989454824999994 |0.0773709636641667   |36.59321714971756 |0.01 |
# MAGIC |2024-05-20 05:00:00|8.072131227777806 |0.8554100720902555   |33.8054992999987  |1.41 |
# MAGIC |2024-05-20 06:00:00|8.412301513888893 |2.3616457910869446   |54.32778184731544 |4.94 |
# MAGIC |2024-05-20 07:00:00|9.190588663888901 |11.20785037204834    |53.04797348656393 |10.44|
# MAGIC +-------------------+------------------+---------------------+------------------+-----+
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - Daily electricity costs
# MAGIC
# MAGIC ##### Background information:
# MAGIC
# MAGIC The energy that is considered to be bought from the electricity market is the difference between the consumed and produced energy.
# MAGIC
# MAGIC To get the hourly cost for the energy bought from the market:<br>
# MAGIC `hourly_cost (€) = hourly_energy_from_market (kWh) * electricity_price_for_hour (€/MWh) / 1000`
# MAGIC
# MAGIC Note, that any consumer buying electricity would also have to pay additional fees (taxes, transfer fees, etc.) that are not considered in this exercise.<br>
# MAGIC And that the given power consumption is only a part of the overall power consumption at Kampusareena.
# MAGIC
# MAGIC ##### Using `hourlyDF` DataFrame from task 6 as a starting point:
# MAGIC
# MAGIC - calculate the average daily temperatures (in `°C`)
# MAGIC - calculate the total daily energy produced by the solar panels (in `kWh`)
# MAGIC - calculate the total daily energy consumed by the water cooling and ventilation machinery (in `kWh`)
# MAGIC - calculate the total daily price for the energy that was bought from the electricity market (in `€`)
# MAGIC
# MAGIC Give the result as a DataFrame where each row contains the date and the corresponding four values rounded to two decimals. Order the DataFrame by the date in chronological order.
# MAGIC
# MAGIC ##### Finally, calculate the total electricity price for the entire week.

# COMMAND ----------

dailyDF: DataFrame = hourlyDF \
    .withColumn(
        "HourlyCost",
        (F.col("ConsumedEnergy") - F.col("ProducedEnergy")) * F.col("Price") / 1000
    ) \
    .withColumn("Date", F.to_date(F.col("Time"))) \
    .groupBy("Date") \
    .agg(
        F.round(F.avg("AvgTemperature"), 2).alias("AvgTemperature"),
        F.round(F.sum("ProducedEnergy"), 2).alias("ProducedEnergy"),
        F.round(F.sum("ConsumedEnergy"), 2).alias("ConsumedEnergy"),
        F.round(F.sum("HourlyCost"), 2).alias("DailyCost")
    ) \
    .orderBy(F.col("Date").asc())


totalPrice: float = dailyDF \
    .select(F.sum(F.col("DailyCost"))) \
    .head()[0]

# COMMAND ----------

dailyDF.show()

print(f"Total price: {totalPrice} €")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example output:
# MAGIC
# MAGIC ```text
# MAGIC +----------+-----------+--------------+--------------+---------+
# MAGIC |      Date|Temperature|ProducedEnergy|ConsumedEnergy|DailyCost|
# MAGIC +----------+-----------+--------------+--------------+---------+
# MAGIC |2024-05-20|      13.02|         373.9|       1084.76|     9.11|
# MAGIC |2024-05-21|      12.91|         369.7|        1154.5|    16.84|
# MAGIC |2024-05-22|      17.75|        355.15|       1708.37|    10.63|
# MAGIC |2024-05-23|      19.79|        360.76|       1948.03|      5.5|
# MAGIC |2024-05-24|      19.68|        258.41|       1978.22|    35.53|
# MAGIC |2024-05-25|      20.79|        294.36|       1533.66|     10.8|
# MAGIC |2024-05-26|       19.9|        265.03|       1264.05|     3.36|
# MAGIC +----------+-----------+--------------+--------------+---------+
# MAGIC
# MAGIC Total price: 91.77 €
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - Theory question
# MAGIC
# MAGIC Using your own words, answer the following questions:
# MAGIC
# MAGIC 1. In this and the previous exercise, you have been using Spark to do the exercise tasks.<br>
# MAGIC    What is Spark, and why is it used in the course?
# MAGIC 2. In general, when can using Spark be beneficial?<br>
# MAGIC    (compared to, for example, handling the data processing with native programming language tools, or other libraries like Pandas)
# MAGIC 3. Are there drawbacks related to using Spark?
# MAGIC
# MAGIC Extensive answers are not required here.<br>
# MAGIC If your answers do not fit into one screen, you have likely written more than what was expected.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. The course is called Data-Intensive Programming, and one of the goals is to teach tools and techniques that can be used to deal with large amounts of data. Spark is a distributed data processing framework that is well suited for processing large datasets. It can be used locally with a single computer (allowing parallelism, i.e., the use of multiple CPU cores for processing datasets), but allows distributing the processing efficiently across multiple computers. It is also currently widely used in the industry.
# MAGIC 2. Using Spark is especially beneficial when working with large datasets that do not fit into the memory or the disk space of a single computer (which might be difficult to handle with, for example, Pandas). And when dealing with slightly smaller datasets, it provides multithreading to the data processing by default, which might not be simple to do with native programming tools, but can speed up the data processing.
# MAGIC 3. For small datasets, using Spark can introduce increased complexity and harder debugging compared to some simpler tools. Also, with small datasets, the performance can be slower compared to tools like Pandas due to overhead in starting and distributing the calculations to the cluster.

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
