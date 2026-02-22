// Databricks notebook source
// MAGIC %md
// MAGIC Copyright 2025 Tampere University<br>
// MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
// MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
// MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

// COMMAND ----------

// MAGIC %md
// MAGIC # COMP.CS.320 - Group assignment - Advanced task 3
// MAGIC
// MAGIC This is the **Scala** version of the optional advanced task 3.<br>
// MAGIC Switch to the Python version, if you want to do the assignment in Python.
// MAGIC
// MAGIC Add your solutions to the cells following the task instructions. You are free to add more cells if you feel it is necessary.<br>
// MAGIC The example outputs are given in a separate notebook in the same folder as this one.
// MAGIC
// MAGIC Look at the notebook for the basic tasks for general information about the group assignment.
// MAGIC
// MAGIC Don't forget to **submit your solutions to Moodle**, [Group assignment submission](https://moodle.tuni.fi/mod/assign/view.php?id=3503812), once your group is finished with the assignment.<br>
// MAGIC Moodle allows multiple submissions, so you can update your work after the initial submission until the deadline.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Short summary on assignment points
// MAGIC
// MAGIC ##### Minimum requirements (points: 0-20 out of maximum of 60):
// MAGIC
// MAGIC - All basic tasks implemented (at least in "a close enough" manner)
// MAGIC - Moodle submission for the group
// MAGIC
// MAGIC ##### For those aiming for higher points (0-60):
// MAGIC
// MAGIC - All basic tasks implemented
// MAGIC - Correct and optimized solutions for the basic tasks (advanced task 1) (0-20 points)
// MAGIC - Two of the other three advanced tasks (2-4) implemented
// MAGIC     - Each graded advanced task will give 0-20 points
// MAGIC     - This notebook is for **advanced task 3**
// MAGIC - Moodle submission for the group

// COMMAND ----------

// imports for the entire notebook
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val studentStoragePath: String = "abfss://students@tunics320f2025gen2.dfs.core.windows.net"

// COMMAND ----------

// returns a list of existing subdirectories under the input path
def getDirectoryList(path: String): Seq[String] = {
    dbutils.fs.ls(path)
        .filter(fileInfo => fileInfo.isDir)
        .map(fileInfo => fileInfo.path)
        .toSeq
        .sorted
}

// remove all files and folders from the target path
def cleanTargetFolder(path: String): Unit = {
    dbutils.fs.rm(path, true)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced Task 3 - Phase 1 - Loading the data
// MAGIC
// MAGIC The folder `assignment/transactions` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains financial dataset with transaction records. The data is based on [https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets) dataset which is made available under Apache License, Version 2.0, [https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0). Only a limited part of the transaction data is included in this task.
// MAGIC
// MAGIC The dataset is divided into 24 parts, which have different file formats and can have slightly differing schemas.
// MAGIC
// MAGIC - The data in `Parquet` format is in the subdirectory `assignment/transactions/parquet`
// MAGIC - The data in `Apache ORC` format is in the subdirectory `assignment/transactions/orc`
// MAGIC - The data in `CSV` format is in the subdirectory `assignment/transactions/csv`
// MAGIC - The data in `JSON` format is in the subdirectory `assignment/transactions/json`
// MAGIC
// MAGIC You are given a helper function, `getDirectoryList`, that can be used to get the paths of the subdirectories under the input path.
// MAGIC
// MAGIC #### The task for phase 1
// MAGIC
// MAGIC - Load the data from all given parts and combine them together using the Delta Lake format. The goal is to write the combined data in Delta format to the [Students container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/students/etag/%220x8DE01A3A1A7F5AB%22/defaultId//publicAccessVal/None).

// COMMAND ----------

// The path for writing the data to the students container
val targetPath: String = s"${studentStoragePath}/change-this-path-to-something-unique-for-your-group"

// this will remove all the files from the target path, i.e., a fresh start
cleanTargetFolder(targetPath)

// COMMAND ----------

???

// COMMAND ----------

// test code for phase 1
val transaction_ids: Seq[Int] = Seq(
    15471290, 15540933, 15614378, 15683708, 15743561, 15813630, 15887875, 15958050,
    16027329, 16097021, 16173489, 16243958, 16313703, 16384244, 16459459, 16529507,
    16605087, 16675317, 16745233, 16815275, 16890288, 16960180, 17030940, 17101718
)

val phase1TestDF: DataFrame = spark.read.format("delta").load(targetPath)
println(s"Total number of transactions: ${phase1TestDF.count()}")
println("Example transactions:")
phase1TestDF
    .filter(col("transaction_id").isin(transaction_ids: _*))
    .orderBy(col("transaction_id").asc)
    .limit(24).show(24, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced Task 3 - Phase 2 - Updating the data
// MAGIC
// MAGIC Four features regarding the transaction information are given in different ways in original data.
// MAGIC
// MAGIC - The timestamps are either given as TimestampType in `timestamp` column, or as StringTypes in `date` and `time` columns.
// MAGIC - The merchant ids are either given as IntegerType in `merchant_id` column, or as StringType in `merchant` column in a different format.
// MAGIC - The merchant state/country location is given in two different ways:
// MAGIC     - either as StringTypes in columns `merchant_state` and `merchang_country`
// MAGIC         - for locations outside the United States, the `merchant_state` will be an empty string in this case
// MAGIC     - or as a single StringType in column `merchant_state`
// MAGIC         - for locations in the United States, the column contains a 2-letter code for the state
// MAGIC         - for locations outside the United States, the column contains the name of the country
// MAGIC - The transaction amount is given either as StringType in `amount` column, or as DoubleType in `amount_dollars` column.
// MAGIC     - The string in the `amount` column is either in format `$64.63` or in format `64.63 USD`. Negative numbers are possible, e.g., `$-12.34` or `12.34 USD`.
// MAGIC
// MAGIC The folder `assignment/transactions/metadata/` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains a CSV file with the information on the US state names and their 2-letter abbreviations.
// MAGIC
// MAGIC #### The task for phase 2
// MAGIC
// MAGIC Update the combined data written in phase 1 with the following:
// MAGIC
// MAGIC - For rows which have the timestamp given with two columns, `date` and `time`, update the `timestamp` column value with the corresponding timestamp value.
// MAGIC - For rows which have the merchant id given as a string in column `merchant`, update the `merchant_id` column value with the corresponding integer value.
// MAGIC - For rows which have the merchant state/country location given in single column, `merchant_state`
// MAGIC     - update the `merchant_country` column with the corresponding country string
// MAGIC     - and update the `merchant_state` column with the full state name for US locations, and with an empty string for non-US locations
// MAGIC - For rows which have the transaction amount given as a string, update the `amount_dollars` column value with the corresponding double value.
// MAGIC
// MAGIC The goal is to have an updated dataset written in Delta format in the target location at the student container.<br>
// MAGIC And all the following columns should have non-null values: `transaction_id`, `timestamp`, `client_id`, `amount_dollars`, `merchant_id`, `merchant_city`, `merchant_state`, `merchant_country`

// COMMAND ----------

???

// COMMAND ----------

// test code for phase 2
val phase2TestDF: DataFrame = spark.read.format("delta").load(targetPath)
    .select("transaction_id", "timestamp", "client_id", "amount_dollars", "merchant_id", "merchant_city", "merchant_state", "merchant_country")

println(s"Total number of transactions: ${phase2TestDF.count()}")
println("Example transactions:")
phase2TestDF
    .filter(col("transaction_id").isin(transaction_ids: _*))
    .orderBy(col("transaction_id").asc)
    .show(24, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced Task 3 - Phase 3 - Data calculations
// MAGIC
// MAGIC The transaction data is mostly US-based, but contains some transactions from other parts of the world.<br>
// MAGIC For this task, the top merchant is the one who had the highest total sales in dollars.
// MAGIC
// MAGIC #### The task for phase 3
// MAGIC
// MAGIC Using the updated data from phase 2
// MAGIC
// MAGIC - Find the top 10 merchants selling in the United States with the following information:
// MAGIC     - number of total transactions
// MAGIC     - the number of US states the merchant has made transactions
// MAGIC     - the US state the merchant had the highest dollar total with the transactions
// MAGIC     - the total dollar amount for all transactions
// MAGIC - Find the top 10 merchants selling outside the United States with the following information:
// MAGIC     - number of total transactions
// MAGIC     - the number of countries the merchant has made transactions
// MAGIC     - the country the merchant had the highest dollar total with the transactions
// MAGIC     - the total dollar amount for all transactions
// MAGIC - Find the merchants that made just a single transaction in France in December 2015 with the following information:
// MAGIC     - the timestamp for the transaction
// MAGIC     - the client's id
// MAGIC     - the dollar amount for the transaction
// MAGIC     - the city the transaction was made in

// COMMAND ----------

val usMerchantsDF: DataFrame = ???

val nonUSMerchantsDF: DataFrame = ???

val franceMerchantsDF: DataFrame = ???

// COMMAND ----------

println("Top 10 merchants selling in the US:")
usMerchantsDF.show(false)

// COMMAND ----------

println("Top 10 merchants selling outside the US:")
nonUSMerchantsDF.show(false)

// COMMAND ----------

println("The merchants having a single transaction in December 2015 in France:")
franceMerchantsDF.show(false)
