// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

package dip25.assignment

import org.apache.spark.sql.SparkSession


trait TaskTrait {
    val tasksImplemented: Boolean
    def run(): Unit

    final val BASIC = "Basic"
    final val ADVANCED = "Advanced"

    // Helper functions to separate the task outputs from each other
    def printTaskLine(taskType: String, taskNumber: Int, subType: String): Unit = {
        val taskTitle: String = taskNumber match {
            case n: Int if n > 0 => subType match {
                case "" => s"${taskType} Task ${n}"
                case _  => s"${taskType} Task ${n} - ${subType}"
            }
            case _ => s"${taskType}"
        }
        val separatingLine: String = "=".repeat(taskTitle.length())
        println(s"${separatingLine}\n${taskTitle}\n${separatingLine}")
    }

    def printTaskLine(taskType: String, taskNumber: Int): Unit = printTaskLine(taskType, taskNumber, "")
    def printTaskLine(taskType: String): Unit = printTaskLine(taskType, 0, "")

    def getSparkSession(): SparkSession = {
        val spark: SparkSession = SparkSession
            .builder()
            .appName("assignment-scala")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .master("local")
            .getOrCreate()

        // suppress informational log messages related to the inner working of Spark
        spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())

        spark
    }
}
