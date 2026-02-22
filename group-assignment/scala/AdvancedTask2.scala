// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

package dip25.assignment

// add other required imports here
import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}


object AdvancedTask2 extends TaskTrait {
    // Change this to true when you have done some implementation of this task
    val tasksImplemented = false

    def run(): Unit = {
        // COMP.CS.320 Data-Intensive Programming, Group assignment, Advanced Task 2
        //
        // The instructions for the advanced task 2 of the group assignment are given in
        // the markdown file "assignment-advanced-2-tasks.md" at the root of the repository.
        // This file contains only the starting code not the instructions.
        //
        // A markdown file "assignment-example-outputs-advanced-2.md" at the root of the repository contains example outputs for the tasks.
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



        printTaskLine(ADVANCED, 2)
        // Advanced Task 2 - Wikipedia articles

        val tenMostFrequentWordsDF: DataFrame = ???

        println("Top 10 most frequent words across all articles:")
        tenMostFrequentWordsDF.show(false)



        val softwareArticles: Seq[String] = ???

        println("Articles in alphabetical order where the word 'software' appears more than 5 times:")
        softwareArticles.foreach(title => println(s"- ${title}"))



        val longestWordsDF: DataFrame = ???

        println("The longest words appearing in at least 10%, 25%, 50%, 75, and 90% of the articles:")
        longestWordsDF.show(false)



        val frequentWordsDF: DataFrame = ???

        println("Top 5 most frequent words per article (excluding forbidden words) in articles last updated before October 2025:")
        frequentWordsDF.show(false)
    }
}
