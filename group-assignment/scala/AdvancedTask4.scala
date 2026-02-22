// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

package dip25.assignment

// add other required imports here
import org.apache.spark.sql.{DataFrame, SparkSession}


object AdvancedTask4 extends TaskTrait {
    // Change this to true when you have done some implementation of this task
    val tasksImplemented = false

    def run(): Unit = {
        // COMP.CS.320 Data-Intensive Programming, Group assignment, Advanced Task 4
        //
        // The instructions for the advanced task 4 of the group assignment are given in
        // the markdown file "assignment-advanced-4-tasks.md" at the root of the repository.
        // This file contains only the starting code not the instructions.
        //
        // A markdown file "assignment-example-outputs-advanced-4.md" at the root of the repository contains example outputs for the tasks.
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



        printTaskLine(ADVANCED, 4, "Case 1")
        // Advanced Task 4 - Case 1 - Predicting the hour of the day
        //
        // It is likely that even using the same seed value, the results will be different compared to the example output.

        val case1Accuracy: Double = ???

        val case1AvgHourDiff: Double = ???


        println(s"The overall accuracy of the hour prediction model: ${scala.math.round(case1Accuracy*10000)/100.0} %")
        println(s"The average hour difference between the predicted and actual hour of the day: ${case1AvgHourDiff}")



        printTaskLine(ADVANCED, 4, "Case 2")
        // Advanced Task 4 - Case 2 - Predicting whether it is a weekend or not

        val case2Accuracy: Double = ???

        val case2AccuracyDF: DataFrame = ???


        println(s"The overall accuracy of the weekend prediction model is ${scala.math.round(case2Accuracy*10000)/100.0} %")
        println("Accuracy (in percentages) of the weekend predictions based on the day of the week:")
        case2AccuracyDF.show(false)



        printTaskLine(ADVANCED, 4, "Case 3")
        // Advanced Task 4 - Case 3 - Predicting whether it is a weekend or not

        val case3Accuracy: Double = ???

        val case3AccuracyDF: DataFrame = ???


        println(s"The overall accuracy of the device type prediction model is ${scala.math.round(case3Accuracy*10000)/100.0} %")
        println("Accuracy (in percentages) of the device predictions based on the device:")
        case3AccuracyDF.show(false)



        printTaskLine(ADVANCED, 4, "Case 4")
        // Advanced Task 4 - Case 4 - Own predictions

        // ???
    }
}
