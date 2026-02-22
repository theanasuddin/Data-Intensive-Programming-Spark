# COMP.CS.320 - Group assignment - Basic tasks

Copyright 2025 Tampere University\
This notebook and software was developed for a Tampere University course COMP.CS.320.\
This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.\
Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

---

## Example outputs for the tasks

This notebook contains example outputs for the basic tasks of the group assignment.

---

### Basic Task 1

```text
The publisher with the highest total video game sales in Japan is: 'Konami'
Sales data for this publisher:
+----+-----------+------------+---------+
|year|japan_total|global_total|ps2_total|
+----+-----------+------------+---------+
|2001|       3.13|       11.58|     5.88|
|2002|       3.42|        8.47|     3.08|
|2003|       3.36|       17.25|     9.46|
|2004|       1.73|       10.72|     6.34|
|2005|       3.37|       13.57|     8.53|
|2006|       4.67|       14.15|     7.65|
|2007|       4.48|        16.9|     6.28|
|2008|       5.59|       27.22|     9.96|
|2009|       4.09|       16.42|     4.26|
|2010|       6.07|       18.45|     1.24|
+----+-----------+------------+---------+
```

### Basic Task 2

```text
The 10 municipalities with the highest buildings per area (postal code) ratio:
+------------+-----+-------+---------+------------------+------------+
|municipality|areas|streets|buildings|buildings_per_area|min_distance|
+------------+-----+-------+---------+------------------+------------+
|Akaa        |8    |879    |12405    |1550.6            |22.4        |
|Pori        |41   |2861   |55620    |1356.6            |64.1        |
|Valkeakoski |10   |759    |12980    |1298.0            |14.5        |
|Jämsä       |25   |1795   |30465    |1218.6            |54.2        |
|Tampere     |40   |2696   |46760    |1169.0            |0.0         |
|Saarijärvi  |20   |768    |20226    |1011.3            |130.5       |
|Keuruu      |19   |1026   |18657    |981.9             |79.4        |
|Forssa      |9    |617    |8659     |962.1             |50.0        |
|Jyväskylä   |51   |3094   |48342    |947.9             |95.9        |
|Kangasala   |24   |1766   |22627    |942.8             |2.6         |
+------------+-----+-------+---------+------------------+------------+
```

Note, that some buildings (identified by building_id) have several addresses (street + house_number) in the dataset.\
This output shows the count for distinct buildings.

### Basic Task 3

```text
The address closest to the average location in Tampere: 'Kesälahdentie 43' at (0.886 km)
The address closest to the average location in Hervanta: 'Sinitaival 6' at (0.023 km)
```

Note, that some buildings (identified by building_id) have several addresses (street + house_number) in the dataset.\
This output shows the result when considering each building only once.

### Basic Task 4

```text
The top 7 goalscorers in Spanish La Liga in season 2017-18:
+-----------------------------------+---------------+-----+
|player                             |club           |goals|
+-----------------------------------+---------------+-----+
|Lionel Andrés Messi Cuccittini     |Barcelona      |34   |
|Cristiano Ronaldo dos Santos Aveiro|Real Madrid    |26   |
|Luis Alberto Suárez Díaz           |Barcelona      |25   |
|Iago Aspas Juncal                  |Celta de Vigo  |22   |
|Cristhian Ricardo Stuani Curbelo   |Girona         |21   |
|Antoine Griezmann                  |Atlético Madrid|19   |
|Maximiliano Gómez González         |Celta de Vigo  |18   |
+-----------------------------------+---------------+-----+
```

and

```text
The top 7 goalscorers in Italian Serie A in season 2017-18:
+----------------------------+--------------+-----+
|player                      |club          |goals|
+----------------------------+--------------+-----+
|Ciro Immobile               |Lazio         |29   |
|Mauro Emanuel Icardi Rivero |Internazionale|29   |
|Paulo Bruno Exequiel Dybala |Juventus      |22   |
|Fabio Quagliarella          |Sampdoria     |19   |
|Dries Mertens               |Napoli        |18   |
|Edin Džeko                  |Roma          |16   |
|Gonzalo Gerardo Higuaín     |Juventus      |16   |
+----------------------------+--------------+-----+
```

These correspond to the statistics on Wikipedia: [https://en.wikipedia.org/wiki/2017-18_La_Liga#Top_goalscorers](https://en.wikipedia.org/wiki/2017%E2%80%9318_La_Liga#Top_goalscorers) and [https://en.wikipedia.org/wiki/2017-18_Serie_A#Top_goalscorers](https://en.wikipedia.org/wiki/2017%E2%80%9318_Serie_A#Top_goalscorers).\
(with one additional goal here for the 7th place in Spain)

### Basic Task 5

```text
The number of matches the Finnish players made an appearance in:
+----------------+-------+
|          player|matches|
+----------------+-------+
|Niklas Moisander|     25|
|  Timo Stavitski|      9|
|  Sauli Väisänen|      7|
| Joel Pohjanpalo|      2|
|    Niki Mäenpää|      0|
+----------------+-------+
```

### Basic Task 6

```text
Players who played in at least 10 matches in two separate competitions:
+----------------------------------+---------+----------------------+--------+----------------------+--------+
|player                            |birthArea|competition1          |matches1|competition2          |matches2|
+----------------------------------+---------+----------------------+--------+----------------------+--------+
|Bryan Dabo                        |France   |French Ligue 1        |16      |Italian Serie A       |10      |
|João Mário Naval da Costa Eduardo |Portugal |Italian Serie A       |14      |English Premier League|13      |
|Lucas Ariel Boyé                  |Argentina|Italian Serie A       |11      |Spanish La Liga       |11      |
|Marc Bartra Aregall               |Spain    |Spanish La Liga       |16      |German Bundesliga     |12      |
|Michy Batshuayi Tunga             |Belgium  |English Premier League|11      |German Bundesliga     |10      |
|Philippe Coutinho Correia         |Brazil   |Spanish La Liga       |18      |English Premier League|14      |
|Pierre-Emerick Aubameyang         |France   |German Bundesliga     |16      |English Premier League|13      |
|Vincent Koziello                  |France   |French Ligue 1        |15      |German Bundesliga     |12      |
+----------------------------------+---------+----------------------+--------+----------------------+--------+
```

### Basic Task 7

```text
Number of teams having at least 20 wins: 19
```

```text
The teams with the most wins in each competition:
+----------------------+---------------+----+
|competition           |team           |wins|
+----------------------+---------------+----+
|English Premier League|Manchester City|32  |
|Italian Serie A       |Juventus       |30  |
|French Ligue 1        |PSG            |29  |
|Spanish La Liga       |Barcelona      |28  |
|German Bundesliga     |Bayern München |27  |
+----------------------+---------------+----+
```
