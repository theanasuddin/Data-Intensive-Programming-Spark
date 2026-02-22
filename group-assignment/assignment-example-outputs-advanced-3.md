# COMP.CS.320 - Group assignment - Advanced task 3

Copyright 2025 Tampere University\
This notebook and software was developed for a Tampere University course COMP.CS.320.\
This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.\
Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

---

## Example outputs for the phases

This notebook contains example outputs for the advanced task 3 of the group assignment.

---

## Advanced task 3 - Phase 1

```text
Total number of transactions: 1216863
Example transactions:
+--------------+-------------------+---------+-----------+-----------+-------------+--------------+----------------+---------------+--------------+----------+-----+
|transaction_id|timestamp          |client_id|amount     |merchant_id|merchant_city|merchant_state|merchant_country|merchant       |amount_dollars|date      |time |
+--------------+-------------------+---------+-----------+-----------+-------------+--------------+----------------+---------------+--------------+----------+-----+
|15471290      |2015-01-01 00:00:00|1133     |$64.63     |30286      |Rochester    |NY            |NULL            |NULL           |NULL          |NULL      |NULL |
|15540933      |2015-01-16 01:00:00|169      |$10.68     |2205       |Sumter       |South Carolina|United States   |NULL           |NULL          |NULL      |NULL |
|15614378      |2015-02-01 02:04:00|1736     |$54.24     |NULL       |Brooklyn     |NY            |NULL            |merchant: 24823|NULL          |NULL      |NULL |
|15683708      |2015-02-16 03:01:00|1676     |$53.08     |NULL       |Dayton       |Ohio          |United States   |merchant: 78454|NULL          |NULL      |NULL |
|15743561      |2015-03-01 04:00:00|1543     |3.90 USD   |22268      |Sherman Oaks |CA            |NULL            |NULL           |NULL          |NULL      |NULL |
|15813630      |2015-03-16 05:00:00|61       |78.02 USD  |5113       |West Harrison|New York      |United States   |NULL           |NULL          |NULL      |NULL |
|15887875      |2015-04-01 06:00:00|986      |1.66 USD   |NULL       |Garden City  |NY            |NULL            |merchant: 14528|NULL          |NULL      |NULL |
|15958050      |2015-04-16 07:00:00|249      |127.02 USD |NULL       |Rialto       |California    |United States   |merchant: 54744|NULL          |NULL      |NULL |
|16027329      |2015-05-01 08:00:00|357      |NULL       |18720      |Boston       |MA            |NULL            |NULL           |386.31        |NULL      |NULL |
|16097021      |2015-05-16 09:00:00|1177     |NULL       |15960      |Newark       |Delaware      |United States   |NULL           |11.81         |NULL      |NULL |
|16173489      |2015-06-01 10:00:00|155      |NULL       |NULL       |Hattiesburg  |MS            |NULL            |merchant: 81833|2.34          |NULL      |NULL |
|16243958      |2015-06-16 11:00:00|373      |NULL       |NULL       |Huntington   |Indiana       |United States   |merchant: 22204|58.41         |NULL      |NULL |
|16313703      |NULL               |316      |$25.87     |28098      |Houston      |TX            |NULL            |NULL           |NULL          |01.07.2015|12:00|
|16384244      |NULL               |373      |$4.54      |23233      |Geneva       |Indiana       |United States   |NULL           |NULL          |16.07.2015|13:00|
|16459459      |NULL               |1053     |$60.38     |NULL       |Orlando      |FL            |NULL            |merchant: 60569|NULL          |01.08.2015|14:00|
|16529507      |NULL               |550      |$49.52     |NULL       |Port Orchard |Washington    |United States   |merchant: 99370|NULL          |16.08.2015|15:00|
|16605087      |NULL               |24       |100.00 USD |27092      |Rockford     |IL            |NULL            |NULL           |NULL          |01.09.2015|16:00|
|16675317      |NULL               |274      |95.18 USD  |59935      |Woodbridge   |Virginia      |United States   |NULL           |NULL          |16.09.2015|17:00|
|16745233      |NULL               |126      |23.36 USD  |NULL       |Drasco       |AR            |NULL            |merchant: 30055|NULL          |01.10.2015|18:00|
|16815275      |NULL               |638      |-219.00 USD|NULL       |Marysville   |Ohio          |United States   |merchant: 44795|NULL          |16.10.2015|19:00|
|16890288      |NULL               |300      |NULL       |78644      |Las Vegas    |NV            |NULL            |NULL           |-137.0        |01.11.2015|20:00|
|16960180      |NULL               |1361     |NULL       |45371      |Detroit      |Michigan      |United States   |NULL           |4.44          |16.11.2015|21:00|
|17030940      |NULL               |467      |NULL       |NULL       |Chattanooga  |TN            |NULL            |merchant: 20561|40.44         |01.12.2015|22:00|
|17101718      |NULL               |1286     |NULL       |NULL       |Buffalo      |New York      |United States   |merchant: 20519|83.43         |16.12.2015|23:01|
+--------------+-------------------+---------+-----------+-----------+-------------+--------------+----------------+---------------+--------------+----------+-----+
```

## Advanced task 3 - Phase 2

```text
Total number of transactions: 1216863
Example transactions:
+--------------+-------------------+---------+--------------+-----------+-------------+--------------+----------------+
|transaction_id|timestamp          |client_id|amount_dollars|merchant_id|merchant_city|merchant_state|merchant_country|
+--------------+-------------------+---------+--------------+-----------+-------------+--------------+----------------+
|15471290      |2015-01-01 00:00:00|1133     |64.63         |30286      |Rochester    |New York      |United States   |
|15540933      |2015-01-16 01:00:00|169      |10.68         |2205       |Sumter       |South Carolina|United States   |
|15614378      |2015-02-01 02:04:00|1736     |54.24         |24823      |Brooklyn     |New York      |United States   |
|15683708      |2015-02-16 03:01:00|1676     |53.08         |78454      |Dayton       |Ohio          |United States   |
|15743561      |2015-03-01 04:00:00|1543     |3.9           |22268      |Sherman Oaks |California    |United States   |
|15813630      |2015-03-16 05:00:00|61       |78.02         |5113       |West Harrison|New York      |United States   |
|15887875      |2015-04-01 06:00:00|986      |1.66          |14528      |Garden City  |New York      |United States   |
|15958050      |2015-04-16 07:00:00|249      |127.02        |54744      |Rialto       |California    |United States   |
|16027329      |2015-05-01 08:00:00|357      |386.31        |18720      |Boston       |Massachusetts |United States   |
|16097021      |2015-05-16 09:00:00|1177     |11.81         |15960      |Newark       |Delaware      |United States   |
|16173489      |2015-06-01 10:00:00|155      |2.34          |81833      |Hattiesburg  |Mississippi   |United States   |
|16243958      |2015-06-16 11:00:00|373      |58.41         |22204      |Huntington   |Indiana       |United States   |
|16313703      |2015-07-01 12:00:00|316      |25.87         |28098      |Houston      |Texas         |United States   |
|16384244      |2015-07-16 13:00:00|373      |4.54          |23233      |Geneva       |Indiana       |United States   |
|16459459      |2015-08-01 14:00:00|1053     |60.38         |60569      |Orlando      |Florida       |United States   |
|16529507      |2015-08-16 15:00:00|550      |49.52         |99370      |Port Orchard |Washington    |United States   |
|16605087      |2015-09-01 16:00:00|24       |100.0         |27092      |Rockford     |Illinois      |United States   |
|16675317      |2015-09-16 17:00:00|274      |95.18         |59935      |Woodbridge   |Virginia      |United States   |
|16745233      |2015-10-01 18:00:00|126      |23.36         |30055      |Drasco       |Arkansas      |United States   |
|16815275      |2015-10-16 19:00:00|638      |-219.0        |44795      |Marysville   |Ohio          |United States   |
|16890288      |2015-11-01 20:00:00|300      |-137.0        |78644      |Las Vegas    |Nevada        |United States   |
|16960180      |2015-11-16 21:00:00|1361     |4.44          |45371      |Detroit      |Michigan      |United States   |
|17030940      |2015-12-01 22:00:00|467      |40.44         |20561      |Chattanooga  |Tennessee     |United States   |
|17101718      |2015-12-16 23:01:00|1286     |83.43         |20519      |Buffalo      |New York      |United States   |
+--------------+-------------------+---------+--------------+-----------+-------------+--------------+----------------+
```

## Advanced task 3 - Phase 3

```text
Top 10 merchants selling in the US:
+-----------+----------------+----------+----------+-------------+
|merchant_id|num_transactions|num_states|best_state|total_dollars|
+-----------+----------------+----------+----------+-------------+
|27092      |60268           |51        |California|5446400.0    |
|60569      |30988           |50        |California|1907866.01   |
|61195      |57786           |51        |California|1243242.16   |
|20561      |20976           |51        |California|931436.23    |
|59935      |62273           |51        |California|919829.54    |
|50783      |30852           |50        |California|793459.14    |
|22204      |35282           |51        |New York  |752007.89    |
|75781      |28477           |48        |Texas     |693337.14    |
|43293      |37074           |50        |California|568183.68    |
|32175      |11058           |50        |Florida   |559354.8     |
+-----------+----------------+----------+----------+-------------+
```

and

```text
Top 10 merchants selling outside the US:
+-----------+----------------+-------------+------------+-------------+
|merchant_id|num_transactions|num_countries|best_country|total_dollars|
+-----------+----------------+-------------+------------+-------------+
|51300      |253             |24           |Mexico      |16497.79     |
|16790      |243             |25           |Mexico      |15721.82     |
|49637      |164             |21           |Mexico      |13656.52     |
|7777       |157             |20           |Mexico      |13168.8      |
|22204      |646             |31           |Mexico      |12969.58     |
|52923      |142             |25           |Mexico      |11571.52     |
|59474      |110             |17           |Mexico      |10648.05     |
|61195      |443             |31           |Canada      |9588.94      |
|46284      |322             |15           |Canada      |8319.08      |
|3558       |80              |16           |France      |6534.37      |
+-----------+----------------+-------------+------------+-------------+
```

and

```text
The merchants having a single transaction in December 2015 in France:
+-----------+-------------------+---------+-------+-------------+
|merchant_id|timestamp          |client_id|dollars|merchant_city|
+-----------+-------------------+---------+-------+-------------+
|95475      |2015-12-15 11:47:00|1840     |2.88   |Paris        |
|60569      |2015-12-20 17:32:00|1840     |90.44  |Paris        |
|44919      |2015-12-20 17:34:00|1840     |16.79  |Paris        |
|86563      |2015-12-21 11:38:00|1840     |4.03   |Paris        |
|20519      |2015-12-22 06:42:00|1147     |6.04   |Paris        |
|2703       |2015-12-22 07:20:00|1147     |10.11  |Paris        |
|48919      |2015-12-23 07:28:00|1147     |82.36  |Paris        |
|49637      |2015-12-27 22:53:00|1560     |131.0  |Paris        |
+-----------+-------------------+---------+-------+-------------+
```
