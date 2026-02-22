// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

package dip25.ex6

// some imports that might be required in the tasks
import io.delta.tables.DeltaTable
import java.nio.file.{Files, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.{Try, Success, Failure}


object Ex6Main extends App {
    // COMP.CS.320 Data-Intensive Programming, Exercise 6
    //
    // This exercise demonstrates different file formats (CSV, Parquet, Delta)
    // and some of the operations that can be used with them.
    // The exercise is in three parts.
    // - Tasks 1-4 concern reading and writing operations with CSV and Parquet
    // - Tasks 5-7 introduces the Delta format
    // - Task 8 is a theory question related to file formats.
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
    // - Chapter 4 and 9 (Section: Common DataFrames and Spark SQL Operations) in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
    //     - There are additional code examples in the related GitHub repository: https://github.com/databricks/LearningSparkV2
    //     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    //       https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc
    // - Apache Spark documentation on all available functions that can be used on DataFrames:
    //   https://spark.apache.org/docs/3.5.6/sql-ref-functions.html
    // The full Spark Scala functions API listing for the functions package might have some additional functions listed that
    // have not been updated in the documentation: https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/sql/functions$.html
    // - Databricks documentation:
    //   - What is Delta Lake?: https://docs.databricks.com/en/delta/index.html
    //   - Delta Lake tutorial: https://docs.databricks.com/en/delta/tutorial.html
    //   - Upsert into Delta Lake: https://docs.databricks.com/en/delta/merge.html#modify-all-unmatched-rows-using-merge
    // - The Delta Spark, Scala DeltaTable documentation: https://docs.delta.io/latest/api/scala/spark/io/delta/tables/DeltaTable.html


    // In Databricks, the Spark session is created automatically, and you should not create it yourself.
	val spark: SparkSession = SparkSession
        .builder()
        .appName("ex6")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local")
        .getOrCreate()

    // suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())

    // reduce the number of shuffle partitions from the default 200 to have more efficient local execution
    spark.conf.set("spark.sql.shuffle.partitions", 8)



    // some helper functions used in this exercise

    def getPathList(path: String): Seq[Path] = {
        Files
            .list(Path.of(path))
            .iterator()
            .asScala
            .toSeq
    }

    def sizeInMB(sizeInBytes: Long): Double = (sizeInBytes.toDouble/1024/1024*100).round/100.0
    def sizeInKB(sizeInBytes: Long): Double = (sizeInBytes.toDouble/1024*100).round/100.0
    def folderSizeInKB(filePath: String): Double =
        sizeInKB(getPathList(filePath).map(file => Files.size(file)).sum)

    // print the files and their sizes from the target path
    def printStorage(path: String): Unit = {
        def getStorageSize(currentPath: String): Long = {
            val fileInformation: Seq[Path] = getPathList(currentPath)
            fileInformation.map(
                file => {
                    if (Files.isDirectory(file)) {
                        getStorageSize(file.toString())
                    }
                    else {
                        println(s"${sizeInMB(Files.size(file))} MB --- ${file}")
                        Files.size(file)
                    }
                }
            ).sum
        }

        val sizeInBytes = getStorageSize(path)
        println(s"Total size: ${sizeInMB(sizeInBytes)} MB")
    }

    // remove all files and folders from the target path
    def cleanTargetFolder(path: String): Unit = {
        Try {
            getPathList(path)
              .foreach(filePath => {
                  if (Files.isDirectory(filePath)) cleanTargetFolder(filePath.toString())
                  Files.delete(filePath)
              })
        } match {
            case Failure(_: java.nio.file.NoSuchFileException) => // the folder did not exist => do nothing
            case Failure(exception) => throw exception
            case Success(_) => // the files were removed successfully => do nothing
        }
    }

    // Print column types in a nice format
    def printColumnTypes(inputDF: DataFrame): Unit = {
        val maxColumnLength: Int = inputDF.columns.map(_.size).max
        inputDF
            .dtypes
            .foreach({case (columnName, columnType) => println(s"${columnName}   ${" " * (maxColumnLength - columnName.size)}${columnType}")})
    }

    // Returns a limited sample of the input data frame
    def getTestDF(
        inputDF: DataFrame,
        ids: Seq[String] = Seq("Z1", "Z2"),
        limitRows: Int = 2,
        idColumn: String = "ID",
        nonOrigIdentifier: String = "_"
    ): DataFrame = {
        val origSample: DataFrame = inputDF
            .filter(!col(idColumn).contains(nonOrigIdentifier))
            .limit(limitRows)
        val extraSample: DataFrame = ids
            .map(id => inputDF
                .filter(col(idColumn).endsWith(id))
                .limit(limitRows)
            )
            .reduceLeft((df1, df2) => df1.union(df2))

        origSample.union(extraSample)
    }



    printTaskLine(1)
    // Task 1 - Read and write data in two formats
    //
    // In the weekly-exercises repository, the "data/ex6" folder contains data about car accidents in the USA.
    // The same data is given in multiple formats, and it is a subset of the dataset in Kaggle:
    //     https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents
    //
    // Part 1
    //
    // Read the data into data frames from both CSV and Parquet source format. The CSV source uses `|` as the column separator and contains header rows.
    // Code for displaying the data and information about the source files is already included.
    //
    // Part 2
    //
    // Write the data from `df_csv` and `df_parquet` to the data folder in both CSV and Parquet formats.
    // Note, since the data in both data frames is the same, you can to use either one as the source when writing it to a new folder (regardless of the target format).

    val source_path: String = "../../data/ex6/"
    val target_path: String = "data/"
    val data_name: String = "accidents"


    val source_csv_folder: String = source_path + s"${data_name}_csv"

    // create and display the data from CSV source
    val df_csv: DataFrame = ???

    df_csv.show(5, false)


    // Typically, a some more suitable file format would be used, like Parquet.
    // With Parquet column format is stored in the file itself, so there is no need for schema inference or explicit schema.
    val source_parquet_folder: String = source_path + s"${data_name}_parquet"

    // create and display the data from Parquet source
    val df_parquet: DataFrame = ???

    df_parquet.show(5, false)


    // print the list of source files for the different file formats
    println("CSV files:")
    printStorage(source_csv_folder)

    println("\nParquet files:")
    printStorage(source_parquet_folder)


    // The schemas for both data frames should be the same (as long as both datasets have been correctly loaded)
    println("===== CSV types =====")
    printColumnTypes(df_csv)
    println("\n===== Parquet types =====")
    printColumnTypes(df_parquet)



    // Example output:
    // ===============
    // (the same output for both data formats):
    //
    // +---------+-------------------+-------------------+---------------------------------------------------------------------------------+---------+------+-----+-------------+
    // |ID       |Start_Time         |End_Time           |Description                                                                      |City     |County|State|Temperature_F|
    // +---------+-------------------+-------------------+---------------------------------------------------------------------------------+---------+------+-----+-------------+
    // |A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Lane blocked.   |Whitehall|Lehigh|PA   |31.0         |
    // |A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Lane blocked.   |Whitehall|Lehigh|PA   |31.0         |
    // |A-3558713|2016-01-14 20:18:33|2017-01-30 13:55:44|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Open.           |Whitehall|Lehigh|PA   |31.0         |
    // |A-3572241|2016-01-14 20:18:33|2017-02-17 23:22:00|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Lane blocked.   |Whitehall|Lehigh|PA   |31.0         |
    // |A-3572395|2016-01-14 20:18:33|2017-02-19 00:38:00|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Traffic problem.|Whitehall|Lehigh|PA   |31.0         |
    // +---------+-------------------+-------------------+---------------------------------------------------------------------------------+---------+------+-----+-------------+
    // only showing top 5 rows
    //
    // and
    //
    // CSV files:
    // 31.97 MB --- ../../data/ex6/accidents_csv/us_traffic_accidents.csv
    // Total size: 31.97 MB
    // Parquet files:
    // 9.56 MB --- ../../data/ex6/accidents_parquet/us_traffic_accidents.parquet
    // Total size: 9.56 MB
    // ===== CSV types =====
    // ID              StringType
    // Start_Time      TimestampType
    // End_Time        TimestampType
    // Description     StringType
    // City            StringType
    // County          StringType
    // State           StringType
    // Temperature_F   DoubleType
    // ===== Parquet types =====
    // ID              StringType
    // Start_Time      TimestampType
    // End_Time        TimestampType
    // Description     StringType
    // City            StringType
    // County          StringType
    // State           StringType
    // Temperature_F   DoubleType


    // remove all previously written files from the target folder first
    cleanTargetFolder(target_path)

    // the target paths for both CSV and Parquet
    val target_file_csv: String = target_path + data_name + "_csv"
    val target_file_parquet: String = target_path + data_name + "_parquet"

    // write the data from part 1 in CSV format to the path given by target_file_csv
    ???

    // write the data from part 1 in Parquet format to the path given by target_file_parquet
    ???


    // Check the written files:
    printStorage(target_file_csv)
    printStorage(target_file_parquet)

    // Both with CSV and Parquet, the data can be divided into multiple files depending on how many workers were doing the writing.
    // If a single file is needed, you can force the output into a single file with "coalesce(1)" before the write command
    // This will make the writing less efficient, especially for larger datasets. (and is not needed in this exercise)
    // There are some additional small metadata files (_SUCCESS, _committed, _started, .crc) that can be ignored in this exercise.


    // Example output:
    // ===============
    // (note that the number of files might be different for you):
    //
    // 0.26 MB --- data/accidents_csv/.part-00000-50466a61-45d5-4b85-8823-c371b2c9a29c-c000.csv.crc
    // 0.0 MB --- data/accidents_csv/._SUCCESS.crc
    // 0.0 MB --- data/accidents_csv/_SUCCESS
    // 33.86 MB --- data/accidents_csv/part-00000-50466a61-45d5-4b85-8823-c371b2c9a29c-c000.csv
    // Total size: 34.12 MB
    // 0.0 MB --- data/accidents_parquet/._SUCCESS.crc
    // 9.55 MB --- data/accidents_parquet/part-00000-22a968f0-9c0e-49c9-9e49-703e42782bf4-c000.snappy.parquet
    // 0.07 MB --- data/accidents_parquet/.part-00000-22a968f0-9c0e-49c9-9e49-703e42782bf4-c000.snappy.parquet.crc
    // 0.0 MB --- data/accidents_parquet/_SUCCESS
    // Total size: 9.63 MB



    printTaskLine(2)
    // Task 2 - Add new rows to storage
    //
    // - Create a new data frame based on the task 1 data that contains the `75` latest incidents (based on the starting time) in the city of `Los Angeles`.
    //     - Append a postfix `_Z1` to the IDs of these Los Angeles incidents, e.g., `A-3666323` should be replaced with `A-3666323_Z1`.
    // - Create a second new data frame based on the task 1 data that contains the `50` oldest incidents (based on the starting time) in the city of `Chicago`.
    //     - Append a postfix `_Z1` also to the IDs of these Chicago incidents, e.g., `A-3499003` should be replaced with `A-3499003_Z1`.
    // - Append the rows from both new data frames to the CSV storage and to the Parquet storage.<br>
    //   I.e., write the new rows in append mode in CSV format to folder given by `target_file_csv` and in Parquet format to folder given by `target_file_parquet`.
    // - Finally, read the data from the storages again to check that the appending was successful.

    val los_angeles_rows: Int = 75
    val chicago_rows: Int = 50

    // New data frame with Los Angeles accidents that will be appended to the storage
    val df_new_rows_los_angeles: DataFrame = ???

    // New data frame with Chicago accidents that will be appended to the storage
    val df_new_rows_chicago: DataFrame = ???


    // print out the first 2 rows of the new data frames
    df_new_rows_los_angeles.show(2)
    df_new_rows_chicago.show(2)


    // Append the new rows to CSV storage:
    // important to consistently use the same header and column separator options when using CSV storage
    ???

    // Append the new rows to Parquet storage:
    ???


    // Read the merged data from the CSV files to check that the new rows have been stored
    val df_new_csv: DataFrame = ???

    // Read the merged data from the Parquet files to check that the new rows have been stored
    val df_new_parquet: DataFrame = ???


    print(s"Old DF had ${df_parquet.count()} rows and we are adding ${los_angeles_rows + chicago_rows} rows ")
    println(s"=> we should have ${df_parquet.count() + los_angeles_rows + chicago_rows} in the merged data.")
    println("==========================================================")
    println(s"Old     CSV DF had ${df_csv.count()} rows and new DF has ${df_new_csv.count()} rows.")
    println(s"Old Parquet DF had ${df_parquet.count()} rows and new DF has ${df_new_parquet.count()} rows.")


    // Example output:
    // ===============
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    // |          ID|         Start_Time|           End_Time|         Description|       City|     County|State|Temperature_F|
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    // |A-3666323_Z1|2023-03-29 05:48:30|2023-03-29 07:55:41|San Diego Fwy S -...|Los Angeles|Los Angeles|   CA|         49.0|
    // |A-3657191_Z1|2023-03-23 11:37:30|2023-03-23 13:45:00|CA-134 W - Ventur...|Los Angeles|Los Angeles|   CA|         58.0|
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    // only showing top 2 rows
    // +------------+-------------------+-------------------+--------------------+-------+------+-----+-------------+
    // |          ID|         Start_Time|           End_Time|         Description|   City|County|State|Temperature_F|
    // +------------+-------------------+-------------------+--------------------+-------+------+-----+-------------+
    // |A-3499003_Z1|2016-06-22 16:10:21|2016-06-22 22:10:21|Between 63rd St/E...|Chicago|  Cook|   IL|         73.0|
    // |A-3501512_Z1|2016-06-30 00:35:46|2016-06-30 06:35:46|Closed between 87...|Chicago|  Cook|   IL|         54.9|
    // +------------+-------------------+-------------------+--------------------+-------+------+-----+-------------+
    // only showing top 2 rows
    //
    // and
    //
    // Old DF had 198082 rows and we are adding 125 rows => we should have 198207 in the merged data.
    // ==========================================================
    // Old     CSV DF had 198082 rows and new DF has 198207 rows.
    // Old Parquet DF had 198082 rows and new DF has 198207 rows.



    printTaskLine(3)
    // Task 3 - Append modified rows
    //
    // In the previous task, appending new rows was successful because the new data had the same schema as the original data.
    // In this task, we try to append data with a modified schema to the CSV and Parquet storages.
    //
    // - Create a new data frame based on all rows from `df_new_rows_los_angeles` and `df_new_rows_chicago` from task 2. The data frame should be modified in the following way:
    //     - The values in the `ID` column should have a postfix `_Z2` instead of `_Z1`. E.g., `A-3877306_Z1` should be replaced with `A-3877306_Z2`.
    //     - A new column `AddedColumn1` should be added with values `"prefix-CITY"` where `CITY` is replaced by the city of the incident.
    //     - A new column `AddedColumn2` should be added with a constant value `"New column"`.
    //     - The column `Temperature_F` should be renamed to `Temperature_C` and the Fahrenheit values should be transformed to Celsius values.
    //         - Example of the temperature transformation: `49.0 °F` = `(49.0 - 32) / 9 * 5 °C` = `9.4444 °C`
    //     - The column `Description` should be dropped.
    // - Then append these modified rows to both the CSV storage and the Parquet storage.

    // A new data frame with modified rows
    val df_modified: DataFrame = ???                                       // Remove a column


    // Check the new data frame:
    println(s"Rows in the new data frame: ${df_modified.count()}")
    println("Schema for the new data frame:")
    printColumnTypes(df_modified)
    println("The first 3 rows:")
    df_modified.limit(3).show()


    // Append the new modified rows to CSV storage:
    ???

    // Append the new modified rows to Parquet storage:
    ???


    // Check the written files at this point:
    printStorage(target_file_csv)
    printStorage(target_file_parquet)



    // Example output:
    // ===============
    // Rows in the new data frame: 125
    // Schema for the new data frame:
    // ID              StringType
    // Start_Time      TimestampType
    // End_Time        TimestampType
    // City            StringType
    // County          StringType
    // State           StringType
    // Temperature_C   DoubleType
    // AddedColumn1    StringType
    // AddedColumn2    StringType
    // The first 3 rows:
    // +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    // |          ID|         Start_Time|           End_Time|       City|     County|State|     Temperature_C|      AddedColumn1|AddedColumn2|
    // +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    // |A-3666323_Z2|2023-03-29 05:48:30|2023-03-29 07:55:41|Los Angeles|Los Angeles|   CA| 9.444444444444445|prefix-Los Angeles|  New column|
    // |A-3657191_Z2|2023-03-23 11:37:30|2023-03-23 13:45:00|Los Angeles|Los Angeles|   CA|14.444444444444445|prefix-Los Angeles|  New column|
    // |A-3779912_Z2|2023-01-31 00:58:00|2023-01-31 02:16:34|Los Angeles|Los Angeles|   CA|  8.88888888888889|prefix-Los Angeles|  New column|
    // +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    //
    // and
    //
    // 0.0 MB --- data/accidents_csv/.part-00000-e39730da-b1be-44c7-9883-361b4ff95e61-c000.csv.crc
    // 0.26 MB --- data/accidents_csv/.part-00000-50466a61-45d5-4b85-8823-c371b2c9a29c-c000.csv.crc
    // 0.0 MB --- data/accidents_csv/.part-00001-f92df64f-864d-4b0c-8569-6a14e608841d-c000.csv.crc
    // 0.0 MB --- data/accidents_csv/._SUCCESS.crc
    // 0.01 MB --- data/accidents_csv/part-00001-f92df64f-864d-4b0c-8569-6a14e608841d-c000.csv
    // 0.01 MB --- data/accidents_csv/part-00000-e39730da-b1be-44c7-9883-361b4ff95e61-c000.csv
    // 0.01 MB --- data/accidents_csv/part-00000-f92df64f-864d-4b0c-8569-6a14e608841d-c000.csv
    // 0.0 MB --- data/accidents_csv/.part-00001-e39730da-b1be-44c7-9883-361b4ff95e61-c000.csv.crc
    // 0.01 MB --- data/accidents_csv/part-00001-e39730da-b1be-44c7-9883-361b4ff95e61-c000.csv
    // 0.0 MB --- data/accidents_csv/_SUCCESS
    // 33.86 MB --- data/accidents_csv/part-00000-50466a61-45d5-4b85-8823-c371b2c9a29c-c000.csv
    // 0.0 MB --- data/accidents_csv/.part-00000-f92df64f-864d-4b0c-8569-6a14e608841d-c000.csv.crc
    // Total size: 34.16 MB
    // 0.01 MB --- data/accidents_parquet/part-00000-45d9afc6-ad0d-40aa-9fc6-3f36c1060539-c000.snappy.parquet
    // 0.0 MB --- data/accidents_parquet/._SUCCESS.crc
    // 0.0 MB --- data/accidents_parquet/part-00001-45d9afc6-ad0d-40aa-9fc6-3f36c1060539-c000.snappy.parquet
    // 0.0 MB --- data/accidents_parquet/.part-00000-45d9afc6-ad0d-40aa-9fc6-3f36c1060539-c000.snappy.parquet.crc
    // 0.0 MB --- data/accidents_parquet/.part-00001-28e5409c-e762-4561-b146-482b48302352-c000.snappy.parquet.crc
    // 0.0 MB --- data/accidents_parquet/.part-00001-45d9afc6-ad0d-40aa-9fc6-3f36c1060539-c000.snappy.parquet.crc
    // 9.55 MB --- data/accidents_parquet/part-00000-22a968f0-9c0e-49c9-9e49-703e42782bf4-c000.snappy.parquet
    // 0.07 MB --- data/accidents_parquet/.part-00000-22a968f0-9c0e-49c9-9e49-703e42782bf4-c000.snappy.parquet.crc
    // 0.0 MB --- data/accidents_parquet/_SUCCESS
    // 0.01 MB --- data/accidents_parquet/part-00000-28e5409c-e762-4561-b146-482b48302352-c000.snappy.parquet
    // 0.01 MB --- data/accidents_parquet/part-00001-28e5409c-e762-4561-b146-482b48302352-c000.snappy.parquet
    // 0.0 MB --- data/accidents_parquet/.part-00000-28e5409c-e762-4561-b146-482b48302352-c000.snappy.parquet.crc
    // Total size: 9.65 MB
    //
    // As before, the exact number of files could be different.



    printTaskLine(4)
    // Task 4 - Check the merged data
    //
    // In this task, we check the contents of the CSV and Parquet storages after the two data append operations, the new rows with the same schema in task 2, and the new rows with a modified schema in task 3.
    //
    // Part 1:
    // - The task is to first write the code that loads the data from the storages again.
    // - And then run the given test code that shows the number of rows, columns, schema, and some sample rows.
    //
    // Part 2:
    // - Finally, answer the questions in the at the end of this task.

    // Read in the CSV data again from the CSV storage: target_file_csv
    val modified_csv_df: DataFrame = ???


    // CSV should have been broken in some way
    println("===== CSV storage =====")
    println(s"The number of rows should be correct: ${modified_csv_df.count()} (i.e., ${df_csv.count()}+2*${los_angeles_rows + chicago_rows})")
    println(s"However, the original data had ${df_csv.columns.length} columns, inserted data had ${df_modified.columns.length} columns. Afterwards we have ${modified_csv_df.columns.length} columns while we should have ${df_csv.columns.length + 3} distinct columns.")

    // print the schema of the CSV data frame
    printColumnTypes(modified_csv_df)

    // show two example rows from each addition
    getTestDF(modified_csv_df).show()


    // Read in the Parquet data again from the Parquet storage: target_file_parquet
    val modified_parquet_df: DataFrame = ???


    // Parquet should also be broken in some way
    println("===== Parquet storage =====")
    println(s"The count for number of rows might be wrong: ${df_parquet.count()} (should be: ${df_parquet.count()}+2*${los_angeles_rows + chicago_rows})")
    println(s"Actually all ${df_parquet.count()+2*(los_angeles_rows + chicago_rows)} rows should be included but the 2 conflicting schemas can cause the count to be incorrect.")
    println(s"The original data had ${df_parquet.columns.length} columns, inserted data had ${df_modified.columns.length} columns. Afterwards we have ${modified_parquet_df.columns.length} columns while we should have ${df_parquet.columns.length + 3} distinct columns.")

    // Unlike the CSV case, the data types for the columns have not been affected. But some columns are just ignored.
    printColumnTypes(modified_parquet_df)

    // show two example rows from each addition
    getTestDF(modified_parquet_df).show()



    // Example output:
    // ===============
    // For CSV:
    //
    // The number of rows should be correct: 198332 (i.e., 198082+2*125)
    // However, the original data had 8 columns, inserted data had 9 columns. Afterwards we have 8 columns while we should have 11 distinct columns.
    // ID              StringType
    // Start_Time      StringType
    // End_Time        StringType
    // Description     StringType
    // City            StringType
    // County          StringType
    // State           StringType
    // Temperature_F   StringType
    // +------------+--------------------+--------------------+--------------------+-----------+-----------+------------------+------------------+
    // |          ID|          Start_Time|            End_Time|         Description|       City|     County|             State|     Temperature_F|
    // +------------+--------------------+--------------------+--------------------+-----------+-----------+------------------+------------------+
    // |   A-3558690|2016-01-14T22:18:...|2017-01-30T15:25:...|Closed at Fullert...|  Whitehall|     Lehigh|                PA|              31.0|
    // |   A-3558700|2016-01-14T22:18:...|2017-01-30T15:34:...|Closed at Fullert...|  Whitehall|     Lehigh|                PA|              31.0|
    // |A-3666323_Z1|2023-03-29T08:48:...|2023-03-29T10:55:...|San Diego Fwy S -...|Los Angeles|Los Angeles|                CA|              49.0|
    // |A-3657191_Z1|2023-03-23T13:37:...|2023-03-23T15:45:...|CA-134 W - Ventur...|Los Angeles|Los Angeles|                CA|              58.0|
    // |A-3666323_Z2|2023-03-29T08:48:...|2023-03-29T10:55:...|         Los Angeles|Los Angeles|         CA| 9.444444444444445|prefix-Los Angeles|
    // |A-3657191_Z2|2023-03-23T13:37:...|2023-03-23T15:45:...|         Los Angeles|Los Angeles|         CA|14.444444444444445|prefix-Los Angeles|
    // +------------+--------------------+--------------------+--------------------+-----------+-----------+------------------+------------------+
    //
    // and for Parquet (alternative 1):
    //
    // ===== Parquet storage =====
    // The count for number of rows might be wrong: 198082 (should be: 198082+2*125)
    // Actually all 198332 rows should be included but the 2 conflicting schemas can cause the count to be incorrect.
    // The original data had 8 columns, inserted data had 9 columns. Afterwards we have 8 columns while we should have 11 distinct columns.
    // ID              StringType
    // Start_Time      TimestampType
    // End_Time        TimestampType
    // Description     StringType
    // City            StringType
    // County          StringType
    // State           StringType
    // Temperature_F   DoubleType
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    // |          ID|         Start_Time|           End_Time|         Description|       City|     County|State|Temperature_F|
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    // |   A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Closed at Fullert...|  Whitehall|     Lehigh|   PA|         31.0|
    // |   A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Closed at Fullert...|  Whitehall|     Lehigh|   PA|         31.0|
    // |A-3666323_Z1|2023-03-29 05:48:30|2023-03-29 07:55:41|San Diego Fwy S -...|Los Angeles|Los Angeles|   CA|         49.0|
    // |A-3657191_Z1|2023-03-23 11:37:30|2023-03-23 13:45:00|CA-134 W - Ventur...|Los Angeles|Los Angeles|   CA|         58.0|
    // |A-3666323_Z2|2023-03-29 05:48:30|2023-03-29 07:55:41|                NULL|Los Angeles|Los Angeles|   CA|         NULL|
    // |A-3657191_Z2|2023-03-23 11:37:30|2023-03-23 13:45:00|                NULL|Los Angeles|Los Angeles|   CA|         NULL|
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    //
    // Parquet (alternative 2):
    //
    // ===== Parquet storage =====
    // The count for number of rows might be wrong: 198082 (should be: 198082+2*125)
    // Actually all 198332 rows should be included but the 2 conflicting schemas can cause the count to be incorrect.
    // The original data had 8 columns, inserted data had 9 columns. Afterwards we have 9 columns while we should have 11 distinct columns.
    // ID              StringType
    // Start_Time      TimestampType
    // End_Time        TimestampType
    // City            StringType
    // County          StringType
    // State           StringType
    // Temperature_C   DoubleType
    // AddedColumn1    StringType
    // AddedColumn2    StringType
    // +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    // |          ID|         Start_Time|           End_Time|       City|     County|State|     Temperature_C|      AddedColumn1|AddedColumn2|
    // +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    // |   A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|  Whitehall|     Lehigh|   PA|              NULL|              NULL|        NULL|
    // |   A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|  Whitehall|     Lehigh|   PA|              NULL|              NULL|        NULL|
    // |A-3666323_Z1|2023-03-29 05:48:30|2023-03-29 07:55:41|Los Angeles|Los Angeles|   CA|              NULL|              NULL|        NULL|
    // |A-3657191_Z1|2023-03-23 11:37:30|2023-03-23 13:45:00|Los Angeles|Los Angeles|   CA|              NULL|              NULL|        NULL|
    // |A-3666323_Z2|2023-03-29 05:48:30|2023-03-29 07:55:41|Los Angeles|Los Angeles|   CA| 9.444444444444445|prefix-Los Angeles|  New column|
    // |A-3657191_Z2|2023-03-23 11:37:30|2023-03-23 13:45:00|Los Angeles|Los Angeles|   CA|14.444444444444445|prefix-Los Angeles|  New column|
    // +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+


    // - **Did you get similar output for the data in CSV storage? If not, what was the difference?**
    //     - ???
    //
    // - **What is your explanation/guess for why the CSV seems broken and the schema cannot be inferred anymore?**
    //     - ???
    //
    // - **Did you get similar output for the data in Parquet storage, and which of the 2 alternatives? If not, what was the difference?**
    //     - ???
    //
    // - **What is your explanation/guess for why not all 11 distinct columns are included in the data frame in the Parquet case?**
    //     - ???



    printTaskLine(5)
    // ask 5 - Delta - Reading and writing data
    //
    // Delta, https://docs.databricks.com/en/delta/index.html, tables are a storage format that are more advanced. They can be used somewhat like databases.
    // This is not native in Spark, but open format, which is more and more commonly used.
    //
    // Delta is stricter with data. We cannot, for example, have whitespace in column names, as you can have in Parquet and CSV.
    // However, in this exercise, the example data is given with column names where these additional requirements have already been fulfilled.
    // And thus, you don't have to worry about them in this exercise.
    //
    // Delta technically looks more or less like Parquet with some additional metadata files.
    //
    // The task
    // - In this task, read the source data given in Delta format into a data frame.
    // - And then write a copy of the data into the Students container to allow modifications in the following tasks.

    val source_delta_folder: String = source_path + s"${data_name}_delta"

    // Read the original data in Delta format to a data frame
    val df_delta: DataFrame = ???


    println("The original data from the shared container:")
    println(s"== Number or rows: ${df_delta.count()}")
    println("== Columns:")
    printColumnTypes(df_delta)
    println("== Storage files:")
    printStorage(source_delta_folder)


    val target_file_delta: String = target_path + data_name + "_delta"

    // write the data from df_delta using the Delta format to the path given by target_file_delta
    ???


    // Check the written files:
    println("== Target files:")
    printStorage(target_file_delta)


    // Example output:
    // ===============
    // The original data from the shared container:
    // == Number or rows: 198082
    // == Columns:
    // ID              StringType
    // Start_Time      TimestampType
    // End_Time        TimestampType
    // Description     StringType
    // City            StringType
    // County          StringType
    // State           StringType
    // Temperature_F   DoubleType
    // == Storage files:
    // 0.0 MB --- ../../data/ex6/accidents_delta/_delta_log/00000000000000000000.json
    // 0.0 MB --- ../../data/ex6/accidents_delta/_delta_log/00000000000000000000.crc
    // 9.56 MB --- ../../data/ex6/accidents_delta/part-00000-35a096d7-a0ee-439a-85e0-aa78e2935f39-c000.snappy.parquet
    // Total size: 9.56 MB
    // == Target files:
    // 0.0 MB --- data/accidents_delta/_delta_log/.00000000000000000000.json.crc
    // 0.0 MB --- data/accidents_delta/_delta_log/00000000000000000000.json
    // 0.0 MB --- data/accidents_delta/_delta_log/00000000000000000000.crc
    // 0.0 MB --- data/accidents_delta/_delta_log/.00000000000000000000.crc.crc
    // 0.07 MB --- data/accidents_delta/.part-00000-211fb19f-1e18-48dc-b780-eadcc7f25ec7-c000.snappy.parquet.crc
    // 9.55 MB --- data/accidents_delta/part-00000-211fb19f-1e18-48dc-b780-eadcc7f25ec7-c000.snappy.parquet
    // Total size: 9.63 MB



    printTaskLine(6)
    // Task 6 - Delta - Appending data and checking the results
    //
    // - Append the new rows using the same schema, `df_new_rows_los_angeles` and `df_new_rows_chicago` from task 2, to the Delta storage.
    // - Append the new rows using the modified schema, `df_modified` from task 3, to the Delta storage.
    // - Then, read the merged data and study whether the result with Delta is correct without lost or invalid data.

    // Append the new rows using the same schema, from df_new_rows_los_angeles and df_new_rows_chicago, to the Delta storage:
    ???


    // By default, Delta is similar to Parquet in that it assumes the data schema to stay the same. However, we can enable it to handle schema modifications.
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", true)


    // Append the new rows using the modified schema, df_modified, to the Delta storage:
    ???


    // Read the merged data from Delta storage to check that the new rows have been stored
    val modified_delta_df: DataFrame = ???


    println(s"The number of rows should be correct: ${modified_delta_df.count()} (i.e., ${df_delta.count()}+2*${los_angeles_rows + chicago_rows})")
    println(s"The original data had ${df_delta.columns.length} columns, inserted data had ${df_modified.columns.length} columns. Afterwards we have ${modified_delta_df.columns.length} columns which should match the expected ${df_delta.columns.length + 3} distinct columns.")
    println("Delta should handle the merge perfectly. The columns which were not given values are available with NULL values.")

    // show two example rows from each addition
    getTestDF(modified_delta_df).show()


    // Example output:
    // ===============
    // The number of rows should be correct: 198332 (i.e., 198082+2*125)
    // The original data had 8 columns, inserted data had 9 columns. Afterwards we have 11 columns which should match the expected 11 distinct columns.
    // Delta should handle the merge perfectly. The columns which were not given values are available with NULL values.
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+------------------+------------------+------------+
    // |          ID|         Start_Time|           End_Time|         Description|       City|     County|State|Temperature_F|     Temperature_C|      AddedColumn1|AddedColumn2|
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+------------------+------------------+------------+
    // |   A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Closed at Fullert...|  Whitehall|     Lehigh|   PA|         31.0|              NULL|              NULL|        NULL|
    // |   A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Closed at Fullert...|  Whitehall|     Lehigh|   PA|         31.0|              NULL|              NULL|        NULL|
    // |A-3666323_Z1|2023-03-29 05:48:30|2023-03-29 07:55:41|San Diego Fwy S -...|Los Angeles|Los Angeles|   CA|         49.0|              NULL|              NULL|        NULL|
    // |A-3657191_Z1|2023-03-23 11:37:30|2023-03-23 13:45:00|CA-134 W - Ventur...|Los Angeles|Los Angeles|   CA|         58.0|              NULL|              NULL|        NULL|
    // |A-3666323_Z2|2023-03-29 05:48:30|2023-03-29 07:55:41|                NULL|Los Angeles|Los Angeles|   CA|         NULL| 9.444444444444445|prefix-Los Angeles|  New column|
    // |A-3657191_Z2|2023-03-23 11:37:30|2023-03-23 13:45:00|                NULL|Los Angeles|Los Angeles|   CA|         NULL|14.444444444444445|prefix-Los Angeles|  New column|
    // +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+------------------+------------------+------------+



    printTaskLine(7)
    // Task 7 - Delta - Full modifications
    //
    // With CSV or Parquet, editing existing values is not possible without overwriting the entire dataset.
    //
    // Previously, we only added new lines. This way of working only supports adding new data.
    // It does **not** support modifying existing data: updating values or deleting rows.
    // (We could do that manually by adding a primary key and timestamp and always searching for the newest value.)
    //
    // Nevertheless, Delta tables take care of this and many more itself.
    //
    // This task is divided into four parts, with separate instructions for each cell.
    // The first three parts ask for some code, and the final part is to run the given test code.
    //
    // Part 1: In the following cell, add the code to write the `df_delta_small` into Delta storage.

    // Let us first save a smaller data so that it is easier to see what is happening
    val delta_table_file: String = target_path + data_name + "_deltatable_small"

    // create a small 6 row data frame with only 5 columns
    val df_delta_small: DataFrame = getTestDF(modified_delta_df, Seq("Z1"), 3)
        .drop("Description", "End_Time", "County", "AddedColumn1", "AddedColumn2")


    // Write the new small data frame to storage in Delta format to path based on delta_table_file
    ???


    // Create Delta table based on your target folder
    val deltatable: DeltaTable = DeltaTable.forPath(spark, delta_table_file)

    // Show the data originally, before the merge that is done in the next part
    println(s"== Originally, the size of Delta storage is ${folderSizeInKB(delta_table_file)} kB and contains ${deltatable.toDF.count()} rows.")
    deltatable.toDF.sort(desc("Start_Time")).show()


    // Delta tables are more like database type tables. We define them based on data and modify the table itself.
    //
    // We do this by telling Delta what is the primary key of the data. After this we tell it to "merge" new data to the old one.
    // If primary key matches, we update the information. If primary key is new, add a row.
    //
    // Part 2:
    // - In the following cell, add the code to update the `deltatable` with the given updates in `df_delta_update`.
    //   The rows should be updated when the `ID` columns match. And if the id from the update is a new one, a new row should be inserted into the `deltatable`.

    // create a 5 row data frame with the same columns with updated values for the temperature
    val df_delta_update: DataFrame = df_new_rows_los_angeles
        .limit(5)
        .drop("Description", "End_Time", "County")
        .withColumn("Temperature_F", round(random(seed=lit(1)) * 100, 1))


    // code for updating the deltatable with df_delta_update
    ???


    // Show the data after the merge
    println(s"== After merge, the size of Delta storage is ${folderSizeInKB(delta_table_file)} kB and contains ${deltatable.toDF.count()} rows.")
    deltatable.toDF.sort(desc("Start_Time")).show()


    // Part 3: As a second modification to the test data, do the following modifications to the `deltatable`:
    // - Fill out the proper temperature values given in Celsius degrees for column `Temperature_C` for all incidents in the delta table.
    // - Remove all rows where the temperature in Celsius is below `-12 °C`.

    // code for updating the deltatable with the Celsius temperature values
    ???

    // code for removing rows where the temperature is below -12 Celsius degrees from the deltatable
    ???


    // Show the data after the second update
    println(s"== After the second update, the size of Delta storage is ${folderSizeInKB(delta_table_file)} kB and contains ${deltatable.toDF.count()} rows.")
    deltatable.toDF.sort(desc("Start_Time")).show()


    // Part 4: Run the following cell as a demonstration on how the additional and unused data can be removed from the storage.
    //
    // The modifications and removals are actually physically done only once something like the vacuuming shown below is done.
    // Before that, the original data still exist in the original files.


    // We can get rid of additional or unused data from the storage by vacuuming

    // you can print the files before vacuuming by uncommenting the following
    // printStorage(delta_table_file)

    // Typically we do not want to vacuum all the data, only data older than 30 days or so.
    // We need to tell Delta that we really want to do something stupid
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)
    println(s"== Before vacuum, the size of Delta storage is ${folderSizeInKB(delta_table_file)} kB.")
    deltatable.vacuum(0)
    println(s"== After vacuum, the size of Delta storage is ${folderSizeInKB(delta_table_file)} kB.")

    // you can print the files after vacuuming by uncommenting the following
    // (there should be more metadata files (JSON and CRC), but the actual data (Parquet files) are truncated)
    // printStorage(delta_table_file)

    // the vacuuming should not change the actual data
    deltatable.toDF.sort(desc("Start_Time")).show()


    // Example output:
    // ===============
    // The numbers for the files sizes might not match exactly.
    //
    // == Originally, the size of Delta storage is 7.68 kB and contains 6 rows.
    // +------------+-------------------+-----------+-----+-------------+-------------+
    // |          ID|         Start_Time|       City|State|Temperature_F|Temperature_C|
    // +------------+-------------------+-----------+-----+-------------+-------------+
    // |A-3666323_Z1|2023-03-29 05:48:30|Los Angeles|   CA|         49.0|         NULL|
    // |A-3657191_Z1|2023-03-23 11:37:30|Los Angeles|   CA|         58.0|         NULL|
    // |A-3779912_Z1|2023-01-31 00:58:00|Los Angeles|   CA|         48.0|         NULL|
    // |   A-3558690|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         NULL|
    // |   A-3558700|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         NULL|
    // |   A-3558713|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         NULL|
    // +------------+-------------------+-----------+-----+-------------+-------------+
    //
    // and
    //
    // == After merge, the size of Delta storage is 9.58 kB and contains 8 rows.
    // +------------+-------------------+-----------+-----+-------------+-------------+
    // |          ID|         Start_Time|       City|State|Temperature_F|Temperature_C|
    // +------------+-------------------+-----------+-----+-------------+-------------+
    // |A-3666323_Z1|2023-03-29 05:48:30|Los Angeles|   CA|         63.6|         NULL|
    // |A-3657191_Z1|2023-03-23 11:37:30|Los Angeles|   CA|         59.9|         NULL|
    // |A-3779912_Z1|2023-01-31 00:58:00|Los Angeles|   CA|         13.5|         NULL|
    // |A-5230341_Z1|2023-01-30 03:07:00|Los Angeles|   CA|          7.7|         NULL|
    // |A-4842824_Z1|2023-01-29 22:11:00|Los Angeles|   CA|         85.4|         NULL|
    // |   A-3558690|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         NULL|
    // |   A-3558700|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         NULL|
    // |   A-3558713|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         NULL|
    // +------------+-------------------+-----------+-----+-------------+-------------+
    //
    // and
    //
    // == After the second update, the size of Delta storage is 13.72 kB and contains 7 rows.
    // +------------+-------------------+-----------+-----+-------------+-------------+
    // |          ID|         Start_Time|       City|State|Temperature_F|Temperature_C|
    // +------------+-------------------+-----------+-----+-------------+-------------+
    // |A-3666323_Z1|2023-03-29 05:48:30|Los Angeles|   CA|         63.6|         17.6|
    // |A-3657191_Z1|2023-03-23 11:37:30|Los Angeles|   CA|         59.9|         15.5|
    // |A-3779912_Z1|2023-01-31 00:58:00|Los Angeles|   CA|         13.5|        -10.3|
    // |A-4842824_Z1|2023-01-29 22:11:00|Los Angeles|   CA|         85.4|         29.7|
    // |   A-3558690|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         -0.6|
    // |   A-3558700|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         -0.6|
    // |   A-3558713|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         -0.6|
    // +------------+-------------------+-----------+-----+-------------+-------------+
    //
    // and finally
    //
    // == Before vacuum, the size of Delta storage is 13.72 kB.
    // Deleted 4 files and directories in a total of 1 directories.
    // == After vacuum, the size of Delta storage is 6.06 kB.
    // +------------+-------------------+-----------+-----+-------------+-------------+
    // |          ID|         Start_Time|       City|State|Temperature_F|Temperature_C|
    // +------------+-------------------+-----------+-----+-------------+-------------+
    // |A-3666323_Z1|2023-03-29 05:48:30|Los Angeles|   CA|         63.6|         17.6|
    // |A-3657191_Z1|2023-03-23 11:37:30|Los Angeles|   CA|         59.9|         15.5|
    // |A-3779912_Z1|2023-01-31 00:58:00|Los Angeles|   CA|         13.5|        -10.3|
    // |A-4842824_Z1|2023-01-29 22:11:00|Los Angeles|   CA|         85.4|         29.7|
    // |   A-3558690|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         -0.6|
    // |   A-3558700|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         -0.6|
    // |   A-3558713|2016-01-14 20:18:33|  Whitehall|   PA|         31.0|         -0.6|
    // +------------+-------------------+-----------+-----+-------------+-------------+



    printTaskLine(8)
    // Task 8 - Theory question
    //
    // Using your own words, answer the following questions:
    //
    // 1. CSV and Parquet files have been used as the data sources in several exercises.
    //     - What benefits (if any) do you get when the data is stored in Parquet format instead of CSV format?
    //     - Are there drawbacks of using Parquet format over CSV format? When would you choose CSV over Parquet?
    // 2. This exercise considered data in CSV, Parquet, and Delta format.
    //     - What other file formats are supported by Spark?
    //     - Can you use other data sources than files with Spark?<br>
    //       If yes, give some source examples that can be used with Spark.
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
