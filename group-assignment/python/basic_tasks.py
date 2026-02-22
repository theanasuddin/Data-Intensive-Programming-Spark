"""The basic tasks of the group assignment"""

from pyspark.sql import DataFrame, SparkSession

from task_helpers import ADVANCED, BASIC,get_spark_session, print_task_line


tasks_implemented: bool = True

def run():
    """Run the basic tasks."""

    # COMP.CS.320 Data-Intensive Programming, Group assignment, Basic tasks
    #
    # The instructions for the basic tasks of the group assignment are given in
    # the markdown file "assignment-basic-tasks.md" at the root of the repository.
    # This file contains only the starting code not the instructions.
    #
    # A markdown file "assignment-example-outputs-basic.md" at the root of the repository contains example outputs for the tasks.
    #
    # The tasks that can be done in either Scala or Python.
    # This is the Python version intended for local development.
    #
    # For the local development the source data for the tasks can be located in the data folder at the root of the repository.
    # So, instead of accessing the data from the Azure storage container, use the local files as the source.
    #
    # Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    #
    # Don't forget to submit your solutions to Moodle once your group is finished with the assignment.

    # In Databricks, the Spark session is created automatically, and you should not create it yourself.
    spark: SparkSession = get_spark_session()



    print_task_line(BASIC, 1)
    # Basic Task 1 - Video game sales data

    bestJapanPublisher: str = __MISSING__IMPLEMENTATION__

    bestJapanPublisherSales: DataFrame = __MISSING__IMPLEMENTATION__


    print(f"The publisher with the highest total video game sales in Japan is: '{bestJapanPublisher}'")
    print("Sales data for this publisher:")
    bestJapanPublisherSales.show()



    print_task_line(BASIC, 2)
    # Basic Task 2 - Building location data

    import math

    kampusareenaBuildingId: str = "101060573F"

    # returns the distance between points (lat1, lon1) and (lat2, lon2) in kilometers
    # based on https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128
    def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R: float = 6378.1  # radius of Earth in kilometers
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        deltaPhi = math.radians(lat2 - lat1)
        deltaLambda = math.radians(lon2 - lon1)

        a = (
            math.sin(deltaPhi * deltaPhi / 4.0) +
            math.cos(phi1) * math.cos(phi2) * math.sin(deltaLambda * deltaLambda / 4.0)
        )

        return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


    municipalityDF: DataFrame = __MISSING__IMPLEMENTATION__


    print("The 10 municipalities with the highest buildings per area (postal code) ratio:")
    municipalityDF.show(truncate=False)


    print_task_line(BASIC, 3)
    # Basic Task 3 - Finding address for building near average location

    tampereAddress: str = __MISSING__IMPLEMENTATION__
    tampereDistance: float = __MISSING__IMPLEMENTATION__

    hervantaAddress: str = __MISSING__IMPLEMENTATION__
    hervantaDistance: float = __MISSING__IMPLEMENTATION__


    print(f"The address closest to the average location in Tampere: '{tampereAddress}' at ({tampereDistance} km)")
    print(f"The address closest to the average location in Hervanta: '{hervantaAddress}' at ({hervantaDistance} km)")



    print_task_line(BASIC, 4)
    # Basic Task 4 - Football data and the best goalscorers in Spain and Italy

    goalscorersSpainDF: DataFrame = __MISSING__IMPLEMENTATION__

    goalscorersItalyDF: DataFrame = __MISSING__IMPLEMENTATION__


    print("The top 7 goalscorers in Spanish La Liga in season 2017-18:")
    goalscorersSpainDF.show(truncate=False)

    print("The top 7 goalscorers in Italian Serie A in season 2017-18:")
    goalscorersItalyDF.show(truncate=False)



    print_task_line(BASIC, 5)
    # Basic Task 5 - Match appearances for Finnish players

    finnishPlayersDF: DataFrame = __MISSING__IMPLEMENTATION__


    print("The number matches the Finnish players made an appearance in:")
    finnishPlayersDF.show()



    print_task_line(BASIC, 6)
    # Basic Task 6 - Match appearances in multiple competitions

    appearanceDF: DataFrame = __MISSING__IMPLEMENTATION__


    print("Players who played in at least 10 matches in two separate competitions:")
    appearanceDF.show(truncate=False)



    print_task_line(BASIC, 7)
    # Basic Task 7 - Number of wins for teams

    twentyWinsCount: float = __MISSING__IMPLEMENTATION__

    bestTeamsDF: DataFrame = __MISSING__IMPLEMENTATION__


    print(f"Number of teams having at least 20 wins: {twentyWinsCount}")
    print("The teams with the most wins in each competition:")
    bestTeamsDF.show(truncate=False)



    print_task_line(BASIC, 8)
    # Basic Task 8 - General information

    # ???



    print_task_line(ADVANCED, 1)
    # Advanced Task 1 - Optimized and correct solutions to the basic tasks

    # ???
