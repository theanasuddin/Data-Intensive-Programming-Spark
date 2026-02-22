// Databricks notebook source
// MAGIC %md
// MAGIC Copyright 2025 Tampere University<br>
// MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
// MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
// MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

// COMMAND ----------

// MAGIC %md
// MAGIC # COMP.CS.320 - Group assignment - Advanced task 2
// MAGIC
// MAGIC ## Example outputs for the questions
// MAGIC
// MAGIC This notebook contains example outputs for the advanced task 2 of the group assignment.

// COMMAND ----------

// MAGIC %md
// MAGIC ```text
// MAGIC Top 10 most frequent words across all articles:
// MAGIC +------+-----------+
// MAGIC |word  |total_count|
// MAGIC +------+-----------+
// MAGIC |united|13534      |
// MAGIC |which |11287      |
// MAGIC |first |10494      |
// MAGIC |after |9648       |
// MAGIC |states|9621       |
// MAGIC |their |8985       |
// MAGIC |state |7683       |
// MAGIC |trump |7288       |
// MAGIC |during|6835       |
// MAGIC |would |6698       |
// MAGIC +------+-----------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ```text
// MAGIC Articles in alphabetical order where the word 'software' appears more than 5 times:
// MAGIC - Android (operating system)
// MAGIC - History of autism
// MAGIC - Nintendo Switch
// MAGIC - Reorganization plan of United States Army
// MAGIC - Tartan
// MAGIC ```
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ```text
// MAGIC The longest words appearing in at least 10%, 25%, 50%, 75, and 90% of the articles:
// MAGIC +----+------------------+---------------+---------------+-------------+-------------+
// MAGIC |rank|word_10           |word_25        |word_50        |word_75      |word_90      |
// MAGIC +----+------------------+---------------+---------------+-------------+-------------+
// MAGIC |1   |disproportionately|representatives|representatives|international|international|
// MAGIC |2   |categoryamerican  |administration |administration |association  |following    |
// MAGIC |3   |categoryarticles  |administrative |representative |established  |including    |
// MAGIC |4   |responsibilities  |communications |approximately  |independent  |american     |
// MAGIC |5   |unconstitutional  |constitutional |controversial  |information  |national     |
// MAGIC |6   |accomplishments   |implementation |international  |significant  |against      |
// MAGIC |7   |administrations   |infrastructure |investigation  |additional   |another      |
// MAGIC |8   |categoryhistory   |investigations |significantly  |considered   |between      |
// MAGIC |9   |characteristics   |reconstruction |additionally   |government   |history      |
// MAGIC |10  |dissatisfaction   |representation |intelligence   |previously   |several      |
// MAGIC +----+------------------+---------------+---------------+-------------+-------------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ```text
// MAGIC Top 5 most frequent words per article (excluding forbidden words) in articles last updated before October 2025:
// MAGIC +----------------------------------+----------+----------+-----------+----------+----------+----------+--------+
// MAGIC |title                             |date      |characters|word_1     |word_2    |word_3    |word_4    |word_5  |
// MAGIC +----------------------------------+----------+----------+-----------+----------+----------+----------+--------+
// MAGIC |History of Suresnes               |2025-07-29|383598    |suresnes   |which     |paris     |after     |school  |
// MAGIC |Flora of Cuba                     |2025-08-04|381741    |eastern    |hispaniola|western   |griseb    |pwilson |
// MAGIC |Statute Law Revision Act 1873     |2025-08-25|375372    |whole      |ireland   |section   |hundred   |eight   |
// MAGIC |1st Maine Cavalry Regiment        |2025-09-07|350196    |cavalry    |maine     |their     |regiment  |battle  |
// MAGIC |Statute Law Revision Act 1867     |2025-09-11|382340    |whole      |duties    |majesty   |hundred   |thousand|
// MAGIC |Stability and Growth Pact         |2025-09-15|367298    |programme  |balance   |stability |structural|fiscal  |
// MAGIC |Statute Law Revision Act 1863     |2025-09-18|350092    |whole      |statute   |shall     |statutes  |chapter |
// MAGIC |COVID-19 misinformation           |2025-09-20|380506    |coronavirus|virus     |conspiracy|pandemic  |media   |
// MAGIC |Kashmir conflict                  |2025-09-26|363910    |kashmir    |india     |pakistan  |jammu     |indian  |
// MAGIC |Insect paleobiota of Burmese amber|2025-09-28|470187    |amber      |journal   |burmese   |research  |myanmar |
// MAGIC |African humid period              |2025-09-29|374041    |african    |during    |climate   |journal   |humid   |
// MAGIC |Mythology of Benjamin Banneker    |2025-09-29|387149    |banneker   |washington|benjamin  |ellicott  |first   |
// MAGIC +----------------------------------+----------+----------+-----------+----------+----------+----------+--------+
// MAGIC ```
