// Databricks notebook source
// MAGIC %md
// MAGIC Copyright 2025 Tampere University<br>
// MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
// MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
// MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

// COMMAND ----------

// MAGIC %md
// MAGIC # COMP.CS.320 - Group assignment - Basic tasks
// MAGIC
// MAGIC ## Example outputs for the tasks
// MAGIC
// MAGIC This notebook contains example outputs for the basic tasks of the group assignment.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 1
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The publisher with the highest total video game sales in Japan is: 'Konami'
// MAGIC Sales data for this publisher:
// MAGIC +----+-----------+------------+---------+
// MAGIC |year|japan_total|global_total|ps2_total|
// MAGIC +----+-----------+------------+---------+
// MAGIC |2001|       3.13|       11.58|     5.88|
// MAGIC |2002|       3.42|        8.47|     3.08|
// MAGIC |2003|       3.36|       17.25|     9.46|
// MAGIC |2004|       1.73|       10.72|     6.34|
// MAGIC |2005|       3.37|       13.57|     8.53|
// MAGIC |2006|       4.67|       14.15|     7.65|
// MAGIC |2007|       4.48|        16.9|     6.28|
// MAGIC |2008|       5.59|       27.22|     9.96|
// MAGIC |2009|       4.09|       16.42|     4.26|
// MAGIC |2010|       6.07|       18.45|     1.24|
// MAGIC +----+-----------+------------+---------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 2
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The 10 municipalities with the highest buildings per area (postal code) ratio:
// MAGIC +------------+-----+-------+---------+------------------+------------+
// MAGIC |municipality|areas|streets|buildings|buildings_per_area|min_distance|
// MAGIC +------------+-----+-------+---------+------------------+------------+
// MAGIC |Akaa        |8    |879    |12405    |1550.6            |22.4        |
// MAGIC |Pori        |41   |2861   |55620    |1356.6            |64.1        |
// MAGIC |Valkeakoski |10   |759    |12980    |1298.0            |14.5        |
// MAGIC |Jämsä       |25   |1795   |30465    |1218.6            |54.2        |
// MAGIC |Tampere     |40   |2696   |46760    |1169.0            |0.0         |
// MAGIC |Saarijärvi  |20   |768    |20226    |1011.3            |130.5       |
// MAGIC |Keuruu      |19   |1026   |18657    |981.9             |79.4        |
// MAGIC |Forssa      |9    |617    |8659     |962.1             |50.0        |
// MAGIC |Jyväskylä   |51   |3094   |48342    |947.9             |95.9        |
// MAGIC |Kangasala   |24   |1766   |22627    |942.8             |2.6         |
// MAGIC +------------+-----+-------+---------+------------------+------------+
// MAGIC ```
// MAGIC
// MAGIC Note, that some buildings (identified by building_id) have several addresses (street + house_number) in the dataset.<br>
// MAGIC This output shows the count for distinct buildings.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 3
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The address closest to the average location in Tampere: 'Kesälahdentie 43' at (0.886 km)
// MAGIC The address closest to the average location in Hervanta: 'Sinitaival 6' at (0.023 km)
// MAGIC ```
// MAGIC
// MAGIC Note, that some buildings (identified by building_id) have several addresses (street + house_number) in the dataset.<br>
// MAGIC This output shows the result when considering each building only once.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 4
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The top 7 goalscorers in Spanish La Liga in season 2017-18:
// MAGIC +-----------------------------------+---------------+-----+
// MAGIC |player                             |club           |goals|
// MAGIC +-----------------------------------+---------------+-----+
// MAGIC |Lionel Andrés Messi Cuccittini     |Barcelona      |34   |
// MAGIC |Cristiano Ronaldo dos Santos Aveiro|Real Madrid    |26   |
// MAGIC |Luis Alberto Suárez Díaz           |Barcelona      |25   |
// MAGIC |Iago Aspas Juncal                  |Celta de Vigo  |22   |
// MAGIC |Cristhian Ricardo Stuani Curbelo   |Girona         |21   |
// MAGIC |Antoine Griezmann                  |Atlético Madrid|19   |
// MAGIC |Maximiliano Gómez González         |Celta de Vigo  |18   |
// MAGIC +-----------------------------------+---------------+-----+
// MAGIC ```
// MAGIC
// MAGIC <p>and</p>
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The top 7 goalscorers in Italian Serie A in season 2017-18:
// MAGIC +----------------------------+--------------+-----+
// MAGIC |player                      |club          |goals|
// MAGIC +----------------------------+--------------+-----+
// MAGIC |Ciro Immobile               |Lazio         |29   |
// MAGIC |Mauro Emanuel Icardi Rivero |Internazionale|29   |
// MAGIC |Paulo Bruno Exequiel Dybala |Juventus      |22   |
// MAGIC |Fabio Quagliarella          |Sampdoria     |19   |
// MAGIC |Dries Mertens               |Napoli        |18   |
// MAGIC |Edin Džeko                  |Roma          |16   |
// MAGIC |Gonzalo Gerardo Higuaín     |Juventus      |16   |
// MAGIC +----------------------------+--------------+-----+
// MAGIC ```
// MAGIC
// MAGIC These correspond to the statistics on Wikipedia: [https://en.wikipedia.org/wiki/2017-18_La_Liga#Top_goalscorers](https://en.wikipedia.org/wiki/2017%E2%80%9318_La_Liga#Top_goalscorers) and [https://en.wikipedia.org/wiki/2017-18_Serie_A#Top_goalscorers](https://en.wikipedia.org/wiki/2017%E2%80%9318_Serie_A#Top_goalscorers).<br>
// MAGIC (with one additional goal here for the 7th place in Spain)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 5
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The number of matches the Finnish players made an appearance in:
// MAGIC +----------------+-------+
// MAGIC |          player|matches|
// MAGIC +----------------+-------+
// MAGIC |Niklas Moisander|     25|
// MAGIC |  Timo Stavitski|      9|
// MAGIC |  Sauli Väisänen|      7|
// MAGIC | Joel Pohjanpalo|      2|
// MAGIC |    Niki Mäenpää|      0|
// MAGIC +----------------+-------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 6
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC Players who played in at least 10 matches in two separate competitions:
// MAGIC +----------------------------------+---------+----------------------+--------+----------------------+--------+
// MAGIC |player                            |birthArea|competition1          |matches1|competition2          |matches2|
// MAGIC +----------------------------------+---------+----------------------+--------+----------------------+--------+
// MAGIC |Bryan Dabo                        |France   |French Ligue 1        |16      |Italian Serie A       |10      |
// MAGIC |João Mário Naval da Costa Eduardo |Portugal |Italian Serie A       |14      |English Premier League|13      |
// MAGIC |Lucas Ariel Boyé                  |Argentina|Italian Serie A       |11      |Spanish La Liga       |11      |
// MAGIC |Marc Bartra Aregall               |Spain    |Spanish La Liga       |16      |German Bundesliga     |12      |
// MAGIC |Michy Batshuayi Tunga             |Belgium  |English Premier League|11      |German Bundesliga     |10      |
// MAGIC |Philippe Coutinho Correia         |Brazil   |Spanish La Liga       |18      |English Premier League|14      |
// MAGIC |Pierre-Emerick Aubameyang         |France   |German Bundesliga     |16      |English Premier League|13      |
// MAGIC |Vincent Koziello                  |France   |French Ligue 1        |15      |German Bundesliga     |12      |
// MAGIC +----------------------------------+---------+----------------------+--------+----------------------+--------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 7
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC Number of teams having at least 20 wins: 19
// MAGIC ```
// MAGIC ```text
// MAGIC The teams with the most wins in each competition:
// MAGIC +----------------------+---------------+----+
// MAGIC |competition           |team           |wins|
// MAGIC +----------------------+---------------+----+
// MAGIC |English Premier League|Manchester City|32  |
// MAGIC |Italian Serie A       |Juventus       |30  |
// MAGIC |French Ligue 1        |PSG            |29  |
// MAGIC |Spanish La Liga       |Barcelona      |28  |
// MAGIC |German Bundesliga     |Bayern München |27  |
// MAGIC +----------------------+---------------+----+
// MAGIC ```
