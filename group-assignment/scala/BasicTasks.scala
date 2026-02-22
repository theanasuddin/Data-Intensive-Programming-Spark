// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

package dip25.assignment

// add other required imports here
import org.apache.spark.sql.{DataFrame, SparkSession}


object BasicTasks extends TaskTrait {
    val tasksImplemented = true

    def run(): Unit = {
        // COMP.CS.320 Data-Intensive Programming, Group assignment, Basic tasks
        //
        // The instructions for the basic tasks of the group assignment are given in
        // the markdown file "assignment-basic-tasks.md" at the root of the repository.
        // This file contains only the starting code not the instructions.
        //
        // A markdown file "assignment-example-outputs-basic.md" at the root of the repository contains example outputs for the tasks.
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



        printTaskLine(BASIC, 1)
        // Basic Task 1 - Video game sales data

        val bestJapanPublisher: String = ???

        val bestJapanPublisherSales: DataFrame = ???


        println(s"The publisher with the highest total video game sales in Japan is: '${bestJapanPublisher}'")
        println("Sales data for this publisher:")
        bestJapanPublisherSales.show()



        printTaskLine(BASIC, 2)
        // Basic Task 2 - Building location data

        val kampusareenaBuildingId: String = "101060573F"

        // returns the distance between points (lat1, lon1) and (lat2, lon2) in kilometers
        // based on https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128
        def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
            val R: Double = 6378.1  // radius of Earth in kilometers
            val phi1 = scala.math.toRadians(lat1)
            val phi2 = scala.math.toRadians(lat2)
            val deltaPhi = scala.math.toRadians(lat2 - lat1)
            val deltaLambda = scala.math.toRadians(lon2 - lon1)

            val a = scala.math.sin(deltaPhi * deltaPhi / 4.0) +
                scala.math.cos(phi1) * scala.math.cos(phi2) * scala.math.sin(deltaLambda * deltaLambda / 4.0)

            2 * R * scala.math.atan2(scala.math.sqrt(a), scala.math.sqrt(1 - a))
        }


        val municipalityDF: DataFrame = ???


        println("The 10 municipalities with the highest buildings per area (postal code) ratio:")
        municipalityDF.show(false)



        printTaskLine(BASIC, 3)
        // Basic Task 3 - Finding address for building near average location

        val tampereAddress: String = ???
        val tampereDistance: Double = ???

        val hervantaAddress: String = ???
        val hervantaDistance: Double = ???


        println(s"The address closest to the average location in Tampere: '${tampereAddress}' at (${tampereDistance} km)")
        println(s"The address closest to the average location in Hervanta: '${hervantaAddress}' at (${hervantaDistance} km)")



        printTaskLine(BASIC, 4)
        // Basic Task 4 - Football data and the best goalscorers in Spain and Italy

        val goalscorersSpainDF: DataFrame = ???

        val goalscorersItalyDF: DataFrame = ???


        println("The top 7 goalscorers in Spanish La Liga in season 2017-18:")
        goalscorersSpainDF.show(false)

        println("The top 7 goalscorers in Italian Serie A in season 2017-18:")
        goalscorersItalyDF.show(false)



        printTaskLine(BASIC, 5)
        // Basic Task 5 - Match appearances for Finnish players

        val finnishPlayersDF: DataFrame = ???


        println("The number matches the Finnish players made an appearance in:")
        finnishPlayersDF.show()



        printTaskLine(BASIC, 6)
        // Basic Task 6 - Match appearances in multiple competitions

        val appearanceDF: DataFrame = ???


        println("Players who played in at least 10 matches in two separate competitions:")
        appearanceDF.show(false)



        printTaskLine(BASIC, 7)
        // Basic Task 7 - Number of wins for teams

        val twentyWinsCount: Long = ???

        val bestTeamsDF: DataFrame = ???


        println(s"Number of teams having at least 20 wins: ${twentyWinsCount}")
        println("The teams with the most wins in each competition:")
        bestTeamsDF.show(false)



        printTaskLine(BASIC, 8)
        // Basic Task 8 - General information

        // ???



        printTaskLine(ADVANCED, 1)
        // Advanced Task 1 - Optimized and correct solutions to the basic tasks

        // ???
    }
}
