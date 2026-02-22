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
// MAGIC This is the **Scala** version of the optional advanced task 2.<br>
// MAGIC Switch to the Python version, if you want to do the assignment in Python.
// MAGIC
// MAGIC Add your solutions to the cells following the task instructions. You are free to add more cells if you feel it is necessary.<br>
// MAGIC The example outputs are given in a separate notebook in the same folder as this one.
// MAGIC
// MAGIC Look at the notebook for the basic tasks for general information about the group assignment.
// MAGIC
// MAGIC Don't forget to **submit your solutions to Moodle**, [Group assignment submission](https://moodle.tuni.fi/mod/assign/view.php?id=3503812), once your group is finished with the assignment.<br>
// MAGIC Moodle allows multiple submissions, so you can update your work after the initial submission until the deadline.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Short summary on assignment points
// MAGIC
// MAGIC ##### Minimum requirements (points: 0-20 out of maximum of 60):
// MAGIC
// MAGIC - All basic tasks implemented (at least in "a close enough" manner)
// MAGIC - Moodle submission for the group
// MAGIC
// MAGIC ##### For those aiming for higher points (0-60):
// MAGIC
// MAGIC - All basic tasks implemented
// MAGIC - Correct and optimized solutions for the basic tasks (advanced task 1) (0-20 points)
// MAGIC - Two of the other three advanced tasks (2-4) implemented
// MAGIC     - Each graded advanced task will give 0-20 points
// MAGIC     - This notebook is for **advanced task 2**
// MAGIC - Moodle submission for the group

// COMMAND ----------

// imports for the entire notebook
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ## Advanced Task 2 - Wikipedia articles
// MAGIC
// MAGIC The folder `assignment/wikipedia` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains a number of the longest Wikipedia articles ([https://en.wikipedia.org/w/index.php?title=Special:LongPages&limit=500&offset=0](https://en.wikipedia.org/w/index.php?title=Special:LongPages&limit=500&offset=0)).<br>
// MAGIC The most recent versions of the articles were extracted in XML format using Wikipedia's export tool: [https://en.wikipedia.org/wiki/Special:Export](https://en.wikipedia.org/wiki/Special:Export)
// MAGIC
// MAGIC Spark has support for importing XML files directly to DataFrames, [https://spark.apache.org/docs/latest/sql-data-sources-xml.html](https://spark.apache.org/docs/latest/sql-data-sources-xml.html).
// MAGIC
// MAGIC #### Definition for a word to be considered in this task
// MAGIC
// MAGIC A word is to be considered (and included in the counts) in this task if<br>
// MAGIC
// MAGIC - when the following punctuation characters are removed: '`.`', '`,`', '`;`', '`:`', '`!`', '`?`', '`(`', '`)`', '`[`', '`]`', '`{`', '`}`',<br>
// MAGIC - and all letters have been changed to lower case, i.e., `A` -> `a`, ...
// MAGIC
// MAGIC the word fulfils the following conditions:
// MAGIC
// MAGIC - the word contains only letters in the English alphabet: '`a`', ..., '`z`'
// MAGIC - the word is at least 5 letters long
// MAGIC - the word is not the English word for a specific month:<br>
// MAGIC     `january`, `february`, `march`, `april`, `may`, `june`, `july`, `august`, `september`, `october`, `november`, `december`
// MAGIC - the word in not the English word for a specific season: `summer`, `autumn`, `winter`, `spring`
// MAGIC
// MAGIC For example, words `(These` and `country,` would be valid words to consider with these rules (as `these` and `country`).
// MAGIC
// MAGIC In this task, you can assume that each line in an article is separated by the new line character, '`\n`'.<br>
// MAGIC And that each word is separated by a whitespace character, '` `'.
// MAGIC
// MAGIC #### The tasks
// MAGIC
// MAGIC Load the content of the Wikipedia articles, and find the answers to the following questions using the presented criteria for a word:
// MAGIC
// MAGIC - What are the 10 most frequent words across all included articles?
// MAGIC     - Give the answer as a data frame with columns `word` and `total_count`.
// MAGIC - In which articles does the word `software` appear more than 5 times?
// MAGIC     - Give the answer as a list of articles titles.
// MAGIC - What are the 10 longest words that appear in at least 10% of the articles?
// MAGIC     - And the same for at least 25%, 50%, 75%, and 90% of the articles.
// MAGIC     - Words that have the same length should be ranked alphabetically.
// MAGIC     - Give the answer as a data frame with columns `rank`, `word_10`, `word_25`, `word_50`, `word_75`, and `word_90`.
// MAGIC - What are the 5 most frequent words per article in the articles last updated before October 2025?
// MAGIC     - In the answer, include the title and the date for the article, as well as the full character count for the article.
// MAGIC     - Give the answer as a data frame with columns `title`, `date`, `characters`, `word_1`, `word_2`, `word_3`, `word_4`, and `word_5`
// MAGIC         - where `word_1` would correspond to the most frequent word in the article, `word_2` the second most frequent word, ...
// MAGIC
// MAGIC Even though the tasks ask for data frame answers, RDDs or Datasets can be helpful. However, their use is optional, and all the tasks can be completed by only using data frames.

// COMMAND ----------

val tenMostFrequentWordsDF: DataFrame = ???

// COMMAND ----------

println("Top 10 most frequent words across all articles:")
tenMostFrequentWordsDF.show(false)

// COMMAND ----------

val softwareArticles: Seq[String] = ???

// COMMAND ----------

println("Articles in alphabetical order where the word 'software' appears more than 5 times:")
softwareArticles.foreach(title => println(s"- ${title}"))

// COMMAND ----------

val longestWordsDF: DataFrame = ???

// COMMAND ----------

println("The longest words appearing in at least 10%, 25%, 50%, 75, and 90% of the articles:")
longestWordsDF.show(false)

// COMMAND ----------

val frequentWordsDF: DataFrame = ???

// COMMAND ----------

println("Top 5 most frequent words per article (excluding forbidden words) in articles last updated before October 2025:")
frequentWordsDF.show(false)
