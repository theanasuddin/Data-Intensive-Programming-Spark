# COMP.CS.320 - Group assignment - Advanced task 3

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
    - This notebook is for **advanced task 3**
- Moodle submission for the group

---

## Advanced Task 3 - Phase 1 - Loading the data

The folder `assignment/transactions` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains financial dataset with transaction records. The data is based on [https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets) dataset which is made available under Apache License, Version 2.0, [https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0). Only a limited part of the transaction data is included in this task.

The dataset is divided into 24 parts, which have different file formats and can have slightly differing schemas.

- The data in `Parquet` format is in the subdirectory `assignment/transactions/parquet`
- The data in `Apache ORC` format is in the subdirectory `assignment/transactions/orc`
- The data in `CSV` format is in the subdirectory `assignment/transactions/csv`
- The data in `JSON` format is in the subdirectory `assignment/transactions/json`

You are given a helper function, `getDirectoryList`, that can be used to get the paths of the subdirectories under the input path.

#### The task for phase 1

- Load the data from all given parts and combine them together using the Delta Lake format. The goal is to write the combined data in Delta format to the [Students container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/students/etag/%220x8DE01A3A1A7F5AB%22/defaultId//publicAccessVal/None).

## Advanced Task 3 - Phase 2 - Updating the data

Four features regarding the transaction information are given in different ways in original data.

- The timestamps are either given as TimestampType in `timestamp` column, or as StringTypes in `date` and `time` columns.
- The merchant ids are either given as IntegerType in `merchant_id` column, or as StringType in `merchant` column in a different format.
- The merchant state/country location is given in two different ways:
    - either as StringTypes in columns `merchant_state` and `merchang_country`
        - for locations outside the United States, the `merchant_state` will be an empty string in this case
    - or as a single StringType in column `merchant_state`
        - for locations in the United States, the column contains a 2-letter code for the state
        - for locations outside the United States, the column contains the name of the country
- The transaction amount is given either as StringType in `amount` column, or as DoubleType in `amount_dollars` column.
    - The string in the `amount` column is either in format `$64.63` or in format `64.63 USD`. Negative numbers are possible, e.g., `$-12.34` or `12.34 USD`.

The folder `assignment/transactions/metadata/` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains a CSV file with the information on the US state names and their 2-letter abbreviations.

#### The task for phase 2

Update the combined data written in phase 1 with the following:

- For rows which have the timestamp given with two columns, `date` and `time`, update the `timestamp` column value with the corresponding timestamp value.
- For rows which have the merchant id given as a string in column `merchant`, update the `merchant_id` column value with the corresponding integer value.
- For rows which have the merchant state/country location given in single column, `merchant_state`
    - update the `merchant_country` column with the corresponding country string
    - and update the `merchant_state` column with the full state name for US locations, and with an empty string for non-US locations
- For rows which have the transaction amount given as a string, update the `amount_dollars` column value with the corresponding double value.

The goal is to have an updated dataset written in Delta format in the target location at the student container.\
And all the following columns should have non-null values: `transaction_id`, `timestamp`, `client_id`, `amount_dollars`, `merchant_id`, `merchant_city`, `merchant_state`, `merchant_country`

## Advanced Task 3 - Phase 3 - Data calculations

The transaction data is mostly US-based, but contains some transactions from other parts of the world.\
For this task, the top merchant is the one who had the highest total sales in dollars.

#### The task for phase 3

Using the updated data from phase 2

- Find the top 10 merchants selling in the United States with the following information:
    - number of total transactions
    - the number of US states the merchant has made transactions
    - the US state the merchant had the highest dollar total with the transactions
    - the total dollar amount for all transactions
- Find the top 10 merchants selling outside the United States with the following information:
    - number of total transactions
    - the number of countries the merchant has made transactions
    - the country the merchant had the highest dollar total with the transactions
    - the total dollar amount for all transactions
- Find the merchants that made just a single transaction in France in December 2015 with the following information:
    - the timestamp for the transaction
    - the client's id
    - the dollar amount for the transaction
    - the city the transaction was made in
