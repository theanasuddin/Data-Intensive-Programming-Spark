// Databricks notebook source
// MAGIC %md
// MAGIC Copyright 2025 Tampere University<br>
// MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
// MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
// MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

// COMMAND ----------

// MAGIC %md
// MAGIC # COMP.CS.320 - Group assignment - Advanced task 4
// MAGIC
// MAGIC ## Example outputs for the cases
// MAGIC
// MAGIC This notebook contains example outputs for the advanced task 4 of the group assignment.
// MAGIC
// MAGIC All the example outputs have created using the default parameters for classifier with seed value of `123`.<br>
// MAGIC The training and the test dataset split was 80% vs. 20%, and the same seed value was used when creating the split.
// MAGIC
// MAGIC The outputs do not have to match these values exactly.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced task 4 - Case 1
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The overall accuracy of the hour prediction model: 21.14 %
// MAGIC The average hour difference between the predicted and actual hour of the day: 2.87
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced task 4 - Case 2
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The overall accuracy of the weekend prediction model is 78.78 %
// MAGIC Accuracy (in percentages) of the weekend predictions based on the day of the week:
// MAGIC +---------+--------+
// MAGIC |weekday  |accuracy|
// MAGIC +---------+--------+
// MAGIC |Monday   |96.46   |
// MAGIC |Tuesday  |95.1    |
// MAGIC |Wednesday|96.33   |
// MAGIC |Thursday |94.6    |
// MAGIC |Friday   |93.47   |
// MAGIC |Saturday |33.16   |
// MAGIC |Sunday   |36.02   |
// MAGIC +---------+--------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced task 4 - Case 3
// MAGIC <br>
// MAGIC
// MAGIC ```text
// MAGIC The overall accuracy of the device type prediction model is 72.98 %
// MAGIC Accuracy (in percentages) of the device predictions based on the device:
// MAGIC +-------------+--------+
// MAGIC |device_type  |accuracy|
// MAGIC +-------------+--------+
// MAGIC |elevator     |97.02   |
// MAGIC |ventilation  |91.96   |
// MAGIC |ev_charging  |69.46   |
// MAGIC |water_cooling|64.73   |
// MAGIC |solar_plant  |42.36   |
// MAGIC +-------------+--------+
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced task 4 - Case 4
// MAGIC <br>
// MAGIC
// MAGIC Freely chosen case, no example output.
