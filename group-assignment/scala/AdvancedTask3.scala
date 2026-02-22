// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

package dip25.assignment

// add other required imports here
import java.nio.file.{Files, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.{Try, Success, Failure}


object AdvancedTask3 extends TaskTrait {
    // Change this to true when you have done some implementation of this task
    val tasksImplemented = false

    def getPathList(path: String): Seq[Path] = {
        Files
            .list(Path.of(path))
            .iterator()
            .asScala
            .toSeq
    }

    def getDirectoryList(path: String): Seq[String] = {
        getPathList(path)
            .filter(file => Files.isDirectory(file))
            .map(dir => dir.toString)
            .toIndexedSeq
            .sorted
    }

    // remove all files and folders from the target path
    def cleanTargetFolder(path: String): Unit = {
        Try {
            getPathList(path)
                .foreach(filePath => {
                    if (Files.isDirectory(filePath)) cleanTargetFolder(filePath.toString())
                    Files.delete(filePath)
                })
        }
        match {
            case Failure(_: java.nio.file.NoSuchFileException) => // the folder did not exist => do nothing
            case Failure(exception) => throw exception
            case Success(_) => // the files were removed successfully => do nothing
        }
    }


    def run(): Unit = {
        // COMP.CS.320 Data-Intensive Programming, Group assignment, Advanced Task 3
        //
        // The instructions for the advanced task 3 of the group assignment are given in
        // the markdown file "assignment-advanced-3-tasks.md" at the root of the repository.
        // This file contains only the starting code not the instructions.
        //
        // A markdown file "assignment-example-outputs-advanced-3.md" at the root of the repository contains example outputs for the tasks.
        //
        // The tasks that can be done in either Scala or Python.
        // This is the Scala version intended for local development.
        //
        // For the local development the source data for the tasks can be located in the data folder at the root of the repository.
        // So, instead of accessing the data from the Azure storage container, use the local files as the source.
        //
        // Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
        //
        // Don't forget to submit your solutions to Moodle once your group is finished with the assignment.

        val spark: SparkSession = getSparkSession()
        import spark.implicits._



        printTaskLine(ADVANCED, 3, "Phase 1")
        // Advanced Task 3 - Phase 1 - Loading the data
        //
        // For the local version, the goal is to write the combined data in Delta format to local folder "data/transactions-delta".
        // Depending on your localization settings, some of the timestamps might be printed differently than in the example output.

        val targetPath: String = "data/transactions-delta"
        // this will remove all the files from the target path, i.e., a fresh start
        cleanTargetFolder(targetPath)


        ???


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



        printTaskLine(ADVANCED, 3, "Phase 2")
        // Advanced Task 3 - Phase 2 - Updating the data

        ???


        val phase2TestDF: DataFrame = spark.read.format("delta").load(targetPath)
            .select("transaction_id", "timestamp", "client_id", "amount_dollars", "merchant_id", "merchant_city", "merchant_state", "merchant_country")

        println(s"Total number of transactions: ${phase2TestDF.count()}")
        println("Example transactions:")
        phase2TestDF
            .filter(col("transaction_id").isin(transaction_ids: _*))
            .orderBy(col("transaction_id").asc)
            .show(24, false)



        printTaskLine(ADVANCED, 3, "Phase 3")
        // Advanced Task 3 - Phase 3 - Data calculations

        val usMerchantsDF: DataFrame = ???

        val nonUSMerchantsDF: DataFrame = ???

        val franceMerchantsDF: DataFrame = ???


        println(s"Top 10 merchants selling in the US:")
        usMerchantsDF.show(false)

        println(s"Top 10 merchants selling outside the US:")
        nonUSMerchantsDF.show(false)

        println(s"The merchants having a single transaction in December 2015 in France:")
        franceMerchantsDF.show(false)
    }
}
