# COMP.CS.320 - Group assignment - Advanced task 4

Copyright 2025 Tampere University\
This notebook and software was developed for a Tampere University course COMP.CS.320.\
This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.\
Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

---

Add your solutions to the cells following the task instructions. You are free to add more cells if you feel it is necessary.\
The example outputs are given in a separate notebook in the same folder as this one.

Look at the notebook for the basic tasks for general information about the group assignment.

Don't forget to **submit your solutions to Moodle**, [Group assignment submission](https://moodle.tuni.fi/mod/assign/view.php?id=3503812), once your group is finished with the assignment.\
Moodle allows multiple submissions, so you can update your work after the initial submission until the deadline.

## Short summary on assignment points

##### Minimum requirements (points: 0-20 out of maximum of 60):

- All basic tasks implemented (at least in "a close enough" manner)
- Moodle submission for the group

##### For those aiming for higher points (0-60):

- All basic tasks implemented
- Correct and optimized solutions for the basic tasks (advanced task 1) (0-20 points)
- Two of the other three advanced tasks (2-4) implemented
    - Each graded advanced task will give 0-20 points
    - This notebook is for **advanced task 4**
- Moodle submission for the group

## Advanced Task 4 - Case 1 - Predicting the hour of the day

This advanced task involves experimenting with the classifiers provided by the Spark machine learning library. Time series data collected in the ProCem research project is used as the training and test data. Similar data in a slightly different format was used in the last tasks of the weekly exercise 3.

The folder `assignment/kampusareena` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains measurements from Hervanta campus.

The dataset is given in Parquet format, and it contains data from a period of 6 months, from May 2025 to October 2025.\
Each row contains the average of the measured values for a single minute. The following columns are included in the data:

| column name        | column type   | description |
| ------------------ | ------------- | ----------- |
| timestamp          | timestamp     | The timestamp for this row's measurements |
| temperature        | double        | The temperature measured by the weather station on top of Sähkötalo (`°C`) |
| humidity           | double        | The humidity measured by the weather station on top of Sähkötalo (`%`) |
| power_water_cooling_01 | double    | The electricity power consumed by the first water cooling machine on Kampusareena (`W`) |
| power_water_cooling_02 | double    | The electricity power consumed by the second water cooling machine on Kampusareena (`W`) |
| power_ventilation  | double        | The electricity power consumed by the ventilation machinery on Kampusareena (`W`) |
| power_elevator_01  | double        | The electricity power consumed by the first elevator on Kampusareena (`W`) |
| power_elevator_02  | double        | The electricity power consumed by the second elevator on Kampusareena (`W`) |
| power_ev_charging  | double        | The electricity power consumed by the electric vehicle charging station on Kampusareena (`W`) |
| power_solar_plant  | double        | The total electricity power produced by the solar panels on Kampusareena (`W`) |

#### General guide for each case in advanced task 4

- Load the data from the storage to a data frame. (this needs to be done only for the first case, the same data can be reused in later cases)
- Calculate any values that are not yet explicitly available, but are needed for the case.
- Clean the data and remove any rows that contain missing values (i.e., null values), in the columns that are needed for the case.
- Split the dataset into training and test parts.
- Train a machine learning model using a [Random forest classifier](https://spark.apache.org/docs/3.5.6/ml-classification-regression.html#random-forests) with the case-specific inputs and labels.
- Evaluate the accuracy of the trained model using the test part of the dataset according to the case-specific instructions.

In all cases, you are free to choose the training parameters as you wish. However, don't pick parameters that make the training take a very long time (even if it would produce a more accurate model).\
Also, note that it is advisable that while you are building your task code to only use a portion of the full 6-month dataset in the initial experiments.

#### Case 1 - Predicting hour of the day

- Train a model to predict the **hour of the day** based on `temperature`, `humidity`, the `total power consumption`, and the `power produced by the solar panels`.
    - the total power consumption is the sum of the all six power consumption values
- Evaluate the accuracy of the trained model by calculating the accuracy percentage, i.e., how often it predicts the correct value, and by calculating the average hour difference between the predicted and actual hour of the day
    - For the accuracy measurement, you can use the Spark built-in multi-class classification evaluator, or calculate it by yourself using the prediction data frame.
    - For the average hour difference, consider the cyclic nature of the hour of day, i.e., for this case the difference between hours 22 and 3 is 5, the same difference as there would be between hours 17 and 22.

## Advanced Task 4 - Case 2 - Predicting whether it is a weekend or not

- Train a model to predict whether it is a **weekend** (Saturday or Sunday) or a weekday (Monday-Friday) based on five power values:
    - the total water cooling machine power consumption, i.e., the sum of the power consumptions values for the two water cooling machines
    - the ventilation machine power consumption
    - the total elevator power consumption, i.e., the sum of the power consumption values for the two elevators
    - the electric vehicle charging station power consumption
    - the power production value for the solar panels
- Evaluate the accuracy of the trained model by calculating the accuracy percentage, i.e., how often it predicts the correct value, and by calculating how accurate the prediction is for each separate day of the week.
    - For the accuracy measurement, you can use the Spark built-in multi-class classification evaluator, or calculate it by yourself using the prediction data frame.
    - For the separate day of the week accuracy, calculate the accuracy for predictions where the actual day was Monday, and the same for Tuesday, ...

## Advanced Task 4 - Case 3 - Predicting the device type

- Train a model to predict the **device type** based on the two weather values, `temperature` and `humidity`, two timestamp related values, the `hour` and the `month`, and a `power value` from a device.
    - the power values should be divided into five categories, i.e., device types:
        - `elevator`: for the sum of the power consumption values for the two elevators
        - `ev_charging`: for the electric vehicle charging station power consumption
        - `solar_plant`: the power production value for the solar panels
        - `ventilation`: the ventilation machine power consumption
        - `water_cooling`: the sum of the power consumptions values for the two water cooling machines
- Evaluate the accuracy of the trained model by calculating the accuracy percentage, i.e., how often it predicts the correct value, and by calculating how accurate the prediction is for each separate device type.
    - For the accuracy measurement, you can use the Spark built-in multi-class classification evaluator, or calculate it by yourself using the prediction data frame.
    - For the separate device type accuracy, calculate the accuracy for predictions where the power values were for elevators, and the same for the ventilation, ...

## Advanced Task 4 - Case 4 - Own predictions

Create your own case to study with the provided data.

- you can decide on the input information yourself
- you can decide the predicted attribute yourself
- the complexity of the created case should be comparable to the other given cases
- you can try some other classifier besides random forest for this case if you want
- in your answer, explain what you are trying to do
