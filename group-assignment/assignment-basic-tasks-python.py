# Databricks notebook source
# MAGIC %md
# MAGIC Copyright 2025 Tampere University<br>
# MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
# MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
# MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

# COMMAND ----------

# MAGIC %md
# MAGIC # COMP.CS.320 - Group assignment - Basic tasks
# MAGIC
# MAGIC This is the **Python** version of the assignment containing the compulsory basic tasks.<br>
# MAGIC Switch to the Scala version, if you want to do the assignment in Scala.
# MAGIC
# MAGIC In all tasks, add your solutions to the cells following the task instructions. You are free to add more cells if you feel it is necessary.<br>
# MAGIC The example outputs are given in a separate notebook in the same folder as this one.
# MAGIC
# MAGIC Don't forget to **submit your solutions to Moodle**, [Group assignment submission](https://moodle.tuni.fi/mod/assign/view.php?id=3503812), once your group is finished with the assignment.<br>
# MAGIC Moodle allows multiple submissions, so you can update your work after the initial submission until the deadline.
# MAGIC
# MAGIC ## Basic tasks (compulsory)
# MAGIC
# MAGIC There are, in total, eight basic tasks that every group must implement in order to have an accepted assignment.
# MAGIC
# MAGIC There are three separate datasets used in the coding tasks.
# MAGIC
# MAGIC - The basic task 1 deals with video game sales data.
# MAGIC - The basic tasks 2 and 3 use building location dataset.
# MAGIC - The basic tasks 4-7 are done using a dataset containing events from football matches.
# MAGIC - Finally, the basic task 8 asks some information on your assignment working process.
# MAGIC
# MAGIC ## Advanced tasks (optional)
# MAGIC
# MAGIC There are in total of four advanced tasks that can be done to gain some course points. Despite the name, the advanced tasks may or may not be harder than the basic tasks.
# MAGIC
# MAGIC The advanced task 1 asks you to do all the basic tasks in an optimized way. You might gain some points from this without directly trying, by just implementing the basic tasks efficiently. Logic errors and other issues that cause the basic tasks to give wrong results will be considered in the grading of the first advanced task. A maximum of 20 points will be given based on advanced task 1. Both the basic tasks and the advanced task 1 are done using this notebook, and submitted to Moodle together.
# MAGIC
# MAGIC The other three advanced tasks, are separate tasks and their implementation does not affect the grade given for the advanced task 1.<br>
# MAGIC Only two of the three available tasks will be graded, and each graded task can provide a maximum of 20 points to the total.<br>
# MAGIC You can attempt all three tasks, but only submit at most two of them to Moodle.<br>
# MAGIC Otherwise, the group assignment grader will randomly pick two of the tasks and ignore the third.
# MAGIC
# MAGIC - Advanced task 2 asks you to handle text articles extracted from Wikipedia.
# MAGIC - Advanced task 3 asks you to load in data from multiple formats, and then manipulate it using Delta format.
# MAGIC - Advanced task 4 asks you to do some classification related machine learning tasks with Spark.
# MAGIC
# MAGIC It is possible to gain partial points from the advanced tasks. I.e., if you have not completed the task fully but have implemented some part of the task, you might gain some appropriate portion of the points from the task. Logic errors, very inefficient solutions, and other issues will be taken into account in the task grading.
# MAGIC
# MAGIC The advanced tasks 2, 3, and 4 have separate notebooks that contain the actual tasks. The notebooks can be found in the same folder as this one. These advanced tasks are also submitted to Moodle as separate files.
# MAGIC
# MAGIC ## Assignment grading
# MAGIC
# MAGIC Failing to do the basic tasks, means failing the assignment and thus also failing the course!<br>
# MAGIC "A close enough" solutions might be accepted => even if you fail to do some parts of the basic tasks, submit your work to Moodle.
# MAGIC
# MAGIC Accepted assignment submissions will be graded from 0 to 60 points.
# MAGIC
# MAGIC The maximum grade that can be achieved by doing only the basic tasks is 20/60 points (through advanced task 1).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Short summary
# MAGIC
# MAGIC ##### Minimum requirements (points: 0-20 out of maximum of 60):
# MAGIC
# MAGIC - All basic tasks implemented (at least in "a close enough" manner)
# MAGIC - Moodle submission for the group
# MAGIC
# MAGIC ##### For those aiming for higher points (0-60):
# MAGIC
# MAGIC - All basic tasks implemented
# MAGIC - Correct and optimized solutions for the basic tasks (advanced task 1) (0-20 points)
# MAGIC - Two of the other three advanced tasks (2-4) implemented
# MAGIC     - Each graded advanced task will give 0-20 points
# MAGIC - Moodle submission for the group

# COMMAND ----------

# add other required imports here
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 1 - Video game sales data
# MAGIC
# MAGIC The CSV file `assignment/sales/video_game_sales_2024.csv` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains video game sales data.<br>
# MAGIC The data is based on [https://www.kaggle.com/datasets/asaniczka/video-game-sales-2024](https://www.kaggle.com/datasets/asaniczka/video-game-sales-2024) dataset which is made available under the ODC Attribution License, [https://opendatacommons.org/licenses/by/1-0/index.html](https://opendatacommons.org/licenses/by/1-0/index.html). The data used in this task includes only the video games for which at least some sales data is available, and some original columns have been removed.
# MAGIC
# MAGIC Load the data from the CSV file into a data frame. The column headers and the first few data lines should give sufficient information about the source dataset. The numbers in the sales columns are given in millions.
# MAGIC
# MAGIC Using the data, find answers to the following:
# MAGIC
# MAGIC - Which publisher has the highest total sales in video games in Japan, considering games released in years 2001-2010?
# MAGIC - Separating games released in different years and considering only this publisher and only games released in years 2001-2010, what are the total sales, in Japan and globally, for each year? And how much of those global sales were for PlayStation 2 (PS2) games?
# MAGIC     - I.e., what are the total sales in Japan, in total globally, and in total for PS2 games, for video games released by this publisher in year 2001?<br>
# MAGIC       And the same for year 2002? ...
# MAGIC     - If some sales value is empty (i.e., NULL), it can be considered as 0 sales for that game in that region.

# COMMAND ----------

bestJapanPublisher: str = ???

bestJapanPublisherSales: DataFrame = ???

# COMMAND ----------

print(f"The publisher with the highest total video game sales in Japan is: '{bestJapanPublisher}'")
print("Sales data for this publisher:")
bestJapanPublisherSales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 2 - Building location data
# MAGIC
# MAGIC You are given a dataset containing the locations of buildings in Finland. The dataset is a subset from `https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f` (currently only available through the Wayback Machine: [https://web.archive.org/web/20241009075101/https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f](https://web.archive.org/web/20241009075101/https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f)) limited to only postal codes with the first two numbers in the interval 28-44 ([postal codes in Finland](https://www.posti.fi/en/zip-code-search/postal-codes-in-finland)).
# MAGIC
# MAGIC The dataset is in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) at folder `assignment/buildings`. The data is in Parquet format and the column names should be clear enough to understand the contents.
# MAGIC
# MAGIC Using the data, find the 10 municipalities that have the highest ratio of buildings per area within the municipality.
# MAGIC
# MAGIC - Each distinct postal code in the municipality is considered to be a separate area in this task.
# MAGIC - In your answer, include the following information about the 10 municipalities:
# MAGIC     - the municipality name
# MAGIC     - the number of areas within the municipality
# MAGIC     - the number of streets within the municipality
# MAGIC     - the number of buildings within the municipality
# MAGIC     - the building per area ratio
# MAGIC     - the shortest direct distance in kilometers between a building in the municipality and the kampusareena building at Hervanta campus
# MAGIC         - the building id for the kampusareena building is `101060573F`
# MAGIC         - you are given a haversine function that can be used to calculate the direct distance between two coordinate pairs

# COMMAND ----------

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

# COMMAND ----------

municipalityDF: DataFrame = ???

# COMMAND ----------

print("The 10 municipalities with the highest buildings per area (postal code) ratio:")
municipalityDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 3 - Finding address for building near average location
# MAGIC
# MAGIC Using the building location data from basic task 2, consider two subsets of buildings:
# MAGIC
# MAGIC 1. All the buildings in `Tampere`
# MAGIC 2. All the buildings within Hervanta area, i.e., buildings with a postal code of `33720`
# MAGIC
# MAGIC For both cases:
# MAGIC
# MAGIC - find the average coordinates using all the building locations in the subset
# MAGIC     - the latitude for the average coordinates is the average latitude for the buildings
# MAGIC     - the longitude for the average coordinates is the average longitude for the buildings
# MAGIC - find the address (i.e., street + house number) of the building that is closest to the average coordinates
# MAGIC - calculate the distance from the location of the closest building to the average coordinates

# COMMAND ----------

tampereAddress: str = ???
tampereDistance: float = ??

hervantaAddress: str = ???
hervantaDistance: float = ???

# COMMAND ----------

print(f"The address closest to the average location in Tampere: '{tampereAddress}' at ({tampereDistance} km)")
print(f"The address closest to the average location in Hervanta: '{hervantaAddress}' at ({hervantaDistance} km)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 4 - Football data and the best goalscorers in Spain and Italy
# MAGIC
# MAGIC The folder `assignment/football/events` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains information about events in [football](https://en.wikipedia.org/wiki/Association_football) matches during the season 2017-18 in five European top-level leagues: English Premier League, Italian Serie A, Spanish La Liga, German Bundesliga, and French Ligue 1. The data is based on a dataset from [https://figshare.com/collections/Soccer_match_event_dataset/4415000/5](https://figshare.com/collections/Soccer_match_event_dataset/4415000/5). The data is given in Parquet format.
# MAGIC
# MAGIC Additional player related information are given in Parquet format at folder `assignment/football/players`. This dataset contains information about the player names, default roles when playing, and their birth areas.
# MAGIC
# MAGIC #### Background information
# MAGIC
# MAGIC In the considered leagues, a season is played in a double round-robin format where each team plays against all other teams twice. Once as a home team in their own stadium, and once as an away team in the other team's stadium. A season usually starts in August and ends in May.
# MAGIC
# MAGIC Each league match consists of two halves of 45 minutes each. Each half runs continuously, meaning that the clock is not stopped when the ball is out of play. The referee of the match may add some additional time to each half based on game stoppages. \[[https://en.wikipedia.org/wiki/Association_football#90-minute_ordinary_time](https://en.wikipedia.org/wiki/Association_football#90-minute_ordinary_time)\]
# MAGIC
# MAGIC The team that scores more goals than their opponent wins the match.
# MAGIC
# MAGIC **Columns in the event data**
# MAGIC
# MAGIC Each row in the given data represents an event in a specific match. An event can be, for example, a pass, a foul, a shot, or a save attempt.<br>
# MAGIC Simple explanations for the available columns. Not all of these will be needed in this assignment.
# MAGIC
# MAGIC | column name | column type | description |
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | competition | string | The name of the competition |
# MAGIC | season | string | The season the match was played |
# MAGIC | matchId | integer | A unique id for the match |
# MAGIC | eventId | integer | A unique id for the event |
# MAGIC | homeTeam | string | The name of the home team |
# MAGIC | awayTeam | string | The name of the away team |
# MAGIC | event | string | The main category for the event |
# MAGIC | subEvent | string | The subcategory for the event |
# MAGIC | eventTeam | string | The name of the team that initiated the event |
# MAGIC | eventPlayerId | integer | The id for the player who initiated the event, 0 for events not identified to a single player |
# MAGIC | eventPeriod | string | `1H` for events in the first half, `2H` for events in the second half |
# MAGIC | eventTime | double | The event time in seconds counted from the start of the half |
# MAGIC | tags | array of strings | The descriptions of the tags associated with the event |
# MAGIC | startPosition | struct | The event start position given in `x` and `y` coordinates in range \[0,100\] |
# MAGIC | enPosition | struct | The event end position given in `x` and `y` coordinates in range \[0,100\] |
# MAGIC
# MAGIC The used event categories can be seen from `assignment/football/metadata/eventid2name.csv`.<br>
# MAGIC And all available tag descriptions from `assignment/football/metadata/tags2name.csv`.<br>
# MAGIC You don't need to access these files in the assignment, but they can provide context for the following basic tasks that will use the event data.
# MAGIC
# MAGIC Note that there are two events related to each goal that happened in the matches covered by the dataset.
# MAGIC
# MAGIC - One event for the player who scored the goal. This includes possible own goals, i.e., accidentally directing the ball to their own goal.
# MAGIC - One event for the goalkeeper who tried to stop the goal.
# MAGIC
# MAGIC **Columns in the player data**
# MAGIC
# MAGIC Each row represents a single player. All of the columns will not be needed in the assignment.
# MAGIC
# MAGIC | column name  | column type | description |
# MAGIC | ------------ | ----------- | ----------- |
# MAGIC | playerId     | integer     | A unique id for the player |
# MAGIC | firstName    | string      | The first name of the player |
# MAGIC | lastName     | string      | The last name of the player |
# MAGIC | birthArea    | string      | The birth area (nation or similar) of the player |
# MAGIC | role         | string      | The main role of the player, either `Goalkeeper`, `Defender`, `Midfielder`, or `Forward` |
# MAGIC | foot         | string      | The stronger foot of the player |
# MAGIC
# MAGIC #### The task
# MAGIC
# MAGIC Using the given football data
# MAGIC
# MAGIC - Find the 7 players who scored the highest number of goals in `Spanish La Liga` during season `2017-2018`.
# MAGIC - Find the 7 players who scored the highest number of goals in `Italian Serie A` during season `2017-2018`.
# MAGIC
# MAGIC Give the results as DataFrames, which have one row for each player and the following columns:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | player         | string      | The name of the player (first name + last name) |
# MAGIC | team           | string      | The team that the player played for |
# MAGIC | goals          | integer     | The number of goals the player scored |
# MAGIC
# MAGIC In this task, you can assume that all the relevant players played for the same team for the entire season.

# COMMAND ----------

goalscorersSpainDF: DataFrame = ???

goalscorersItalyDF: DataFrame = ???

# COMMAND ----------

print("The top 7 goalscorers in Spanish La Liga in season 2017-18:")
goalscorersSpainDF.show(truncate=False)

# COMMAND ----------

print("The top 7 goalscorers in Italian Serie A in season 2017-18:")
goalscorersItalyDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 5 - Match appearances for Finnish players
# MAGIC
# MAGIC For this and the following task, a player is considered to have made an appearance in a match if,<br>
# MAGIC considering only events `Shot`, `Pass`, `Free Kick`, and `Save attempt`, the player has participated in at least 3 events in the match.
# MAGIC
# MAGIC Using the football data, find how many match appearances the Finnish players included in the player dataset made in season `2017-2018` considering all the available leagues.<br>
# MAGIC (for this task, the player is considered a Finnish player if their birth area is `Finland`)
# MAGIC
# MAGIC Give the results as a DataFrame, which have one row for each player and the following columns:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | player         | string      | The name of the player (first name + last name) |
# MAGIC | matches        | integer     | The number of matches the player made an appearance |

# COMMAND ----------

finnishPlayersDF: DataFrame = ???

# COMMAND ----------

print("The number of matches the Finnish players made an appearance in:")
finnishPlayersDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 6 - Match appearances in multiple competitions
# MAGIC
# MAGIC In a single match, a player can naturally play for only one team. However, during the 2017-18 season several players were transferred or loaned to another team, and then made appearances for different teams and even in different competitions.
# MAGIC
# MAGIC Using the football data and the definition of a match appearance from basic task 5
# MAGIC
# MAGIC - Find the players who made at least 10 appearances in two separate competitions during season `2017-2018`.
# MAGIC
# MAGIC Give the results as a DataFrame, which have one row for each player and the following columns:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | player         | string      | The name of the player (first name + last name) |
# MAGIC | birthArea      | string      | The birth area (nation or similar) of the player |
# MAGIC | competition1   | string      | The name of the competition the player made the most appearances |
# MAGIC | matches1       | integer     | The number of competition1 matches the player made an appearance |
# MAGIC | competition2   | string      | The name of the competition the player made the second most appearances |
# MAGIC | matches2       | integer     | The number of competition2 matches the player made an appearance |
# MAGIC
# MAGIC For this task, you can assume that no player played matches in more than two competitions during season 2017-18.<br>
# MAGIC If the number of match appearances are equal, `competition1` should be the competition that is first in alphabetical order.

# COMMAND ----------

appearanceDF: DataFrame = ???

# COMMAND ----------

print("Players who played in at least 10 matches in two separate competitions:")
appearanceDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 7 - Number of wins for teams
# MAGIC
# MAGIC Using the football data, calculate how many match wins each team achieved during season `2017-2018`.<br>
# MAGIC And then
# MAGIC
# MAGIC - Find out how many teams had at least 20 match wins during the season.
# MAGIC - For each competition, find the teams that had the most match wins in that competition.
# MAGIC
# MAGIC For the second part, give the results as a DataFrame, which have one row for each competition and the following columns:
# MAGIC
# MAGIC | column name  | column type | description |
# MAGIC | ------------ | ----------- | ----------- |
# MAGIC | competition  | string      | The name of the competition |
# MAGIC | team         | string      | The name of the team|
# MAGIC | wins         | integer     | The number of match wins achieved by the team |
# MAGIC
# MAGIC You can assume that all teams achieved at least one match win during the season.

# COMMAND ----------

twentyWinsCount: float = ???

bestTeamsDF: DataFrame = ???

# COMMAND ----------

print(f"Number of teams having at least 20 wins: {twentyWinsCount}")
print("The teams with the most wins in each competition:")
bestTeamsDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 8 - General information
# MAGIC
# MAGIC Answer the following questions.
# MAGIC
# MAGIC Remember that using AI and collaborating with other students outside your group is allowed as long as the usage and collaboration is documented.<br>
# MAGIC However, every member of the group should have some contribution to the assignment work.
# MAGIC
# MAGIC 1. Who were your group members and their contributions to the work?
# MAGIC     - Solo groups can ignore this question.
# MAGIC
# MAGIC 2. Did you use AI tools while doing the assignment?
# MAGIC     - Which ones, did they help, and how did they help?
# MAGIC
# MAGIC 3. Did you work with students outside your assignment group?
# MAGIC     - Who or which group? And to what extent? (only extensive collaboration need to reported)

# COMMAND ----------

# MAGIC %md
# MAGIC ???

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Task 1 - Optimized and correct solutions to the basic tasks
# MAGIC
# MAGIC - This advanced task 1 will be graded for every group with 0-20 course points.
# MAGIC
# MAGIC Use the tools Spark offers effectively and avoid unnecessary operations in the code for the basic tasks.
# MAGIC
# MAGIC A couple of things to consider (**not** a complete list):
# MAGIC
# MAGIC - Consider using explicit schemas when dealing with CSV data sources.
# MAGIC - Consider only including those columns from a data source that are actually needed.
# MAGIC - Filter unnecessary rows whenever possible to get smaller datasets.
# MAGIC - Avoid collect or similar expensive operations for large datasets.
# MAGIC - Consider using explicit caching if some data frame is used repeatedly.
# MAGIC - Avoid unnecessary shuffling (for example, grouping, joining, or sorting) operations.
# MAGIC - Avoid unnecessary actions (count, show, etc.) that are not needed for the task.
# MAGIC
# MAGIC In addition to the effectiveness of your solutions, the correctness of the solution logic will be considered when determining the grade for advanced task 1.
# MAGIC "A close enough" solution with some logic fails might be enough to have an accepted group assignment, but those failings are likely to lower the score for this task. Errors that prevent the grader for running your code without modifications, can be severely penalized.
# MAGIC
# MAGIC It is okay to have your own test code that would fall into category of "ineffective usage" or "unnecessary operations" while doing the assignment tasks. However, for the final Moodle submission you should comment out or delete such code (and test that you have not broken anything when doing the final modifications).
# MAGIC
# MAGIC Note, that you should not do the basic tasks again for this advanced task, but instead modify your basic task code with more efficient versions.
# MAGIC
# MAGIC You are encouraged to create a text cell below this one and describe what optimizations you have done.<br>
# MAGIC This might help the grader to better recognize how skilled your work with the basic tasks has been.
