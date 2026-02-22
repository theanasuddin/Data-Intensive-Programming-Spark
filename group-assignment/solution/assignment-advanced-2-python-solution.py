# Databricks notebook source
# MAGIC %md
# MAGIC Copyright 2025 Tampere University<br>
# MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
# MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
# MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

# COMMAND ----------

# MAGIC %md
# MAGIC # COMP.CS.320 - Group assignment - Advanced task 2
# MAGIC
# MAGIC This is the **Python** version of the optional advanced task 2.<br>
# MAGIC Switch to the Scala version, if you want to do the assignment in Scala.
# MAGIC
# MAGIC Add your solutions to the cells following the task instructions. You are free to add more cells if you feel it is necessary.<br>
# MAGIC The example outputs are given in a separate notebook in the same folder as this one.
# MAGIC
# MAGIC Look at the notebook for the basic tasks for general information about the group assignment.
# MAGIC
# MAGIC Don't forget to **submit your solutions to Moodle**, [Group assignment submission](https://moodle.tuni.fi/mod/assign/view.php?id=3503812), once your group is finished with the assignment.<br>
# MAGIC Moodle allows multiple submissions, so you can update your work after the initial submission until the deadline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Short summary on assignment points
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
# MAGIC     - This notebook is for **advanced task 2**
# MAGIC - Moodle submission for the group

# COMMAND ----------

# imports for the entire notebook
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    split,
    lower,
    translate,
    regexp_replace,
    length,
    trim,
    array_contains,
    size,
    countDistinct,
    count as spark_count,
    sum as spark_sum,
    when,
    lit,
    expr,
    udf,
    ceil,
    to_timestamp,
    date_format,
    row_number,
)
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Task 2 - Wikipedia articles
# MAGIC
# MAGIC The folder `assignment/wikipedia` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains a number of the longest Wikipedia articles ([https://en.wikipedia.org/w/index.php?title=Special:LongPages&limit=500&offset=0](https://en.wikipedia.org/w/index.php?title=Special:LongPages&limit=500&offset=0)).<br>
# MAGIC The most recent versions of the articles were extracted in XML format using Wikipedia's export tool: [https://en.wikipedia.org/wiki/Special:Export](https://en.wikipedia.org/wiki/Special:Export)
# MAGIC
# MAGIC Spark has support for importing XML files directly to DataFrames, [https://spark.apache.org/docs/latest/sql-data-sources-xml.html](https://spark.apache.org/docs/latest/sql-data-sources-xml.html).
# MAGIC
# MAGIC #### Definition for a word to be considered in this task
# MAGIC
# MAGIC A word is to be considered (and included in the counts) in this task if<br>
# MAGIC
# MAGIC - when the following punctuation characters are removed: '`.`', '`,`', '`;`', '`:`', '`!`', '`?`', '`(`', '`)`', '`[`', '`]`', '`{`', '`}`',<br>
# MAGIC - and all letters have been changed to lower case, i.e., `A` -> `a`, ...
# MAGIC
# MAGIC the word fulfils the following conditions:
# MAGIC
# MAGIC - the word contains only letters in the English alphabet: '`a`', ..., '`z`'
# MAGIC - the word is at least 5 letters long
# MAGIC - the word is not the English word for a specific month:<br>
# MAGIC     `january`, `february`, `march`, `april`, `may`, `june`, `july`, `august`, `september`, `october`, `november`, `december`
# MAGIC - the word in not the English word for a specific season: `summer`, `autumn`, `winter`, `spring`
# MAGIC
# MAGIC For example, words `(These` and `country,` would be valid words to consider with these rules (as `these` and `country`).
# MAGIC
# MAGIC In this task, you can assume that each line in an article is separated by the new line character, '`\n`'.<br>
# MAGIC And that each word is separated by a whitespace character, '` `'.
# MAGIC
# MAGIC #### The tasks
# MAGIC
# MAGIC Load the content of the Wikipedia articles, and find the answers to the following questions using the presented criteria for a word:
# MAGIC
# MAGIC - What are the 10 most frequent words across all included articles?
# MAGIC     - Give the answer as a data frame with columns `word` and `total_count`.
# MAGIC - In which articles does the word `software` appear more than 5 times?
# MAGIC     - Give the answer as a list of articles titles.
# MAGIC - What are the 10 longest words that appear in at least 10% of the articles?
# MAGIC     - And the same for at least 25%, 50%, 75%, and 90% of the articles.
# MAGIC     - Words that have the same length should be ranked alphabetically.
# MAGIC     - Give the answer as a data frame with columns `rank`, `word_10`, `word_25`, `word_50`, `word_75`, and `word_90`.
# MAGIC - What are the 5 most frequent words per article in the articles last updated before October 2025?
# MAGIC     - In the answer, include the title and the date for the article, as well as the full character count for the article.
# MAGIC     - Give the answer as a data frame with columns `title`, `date`, `characters`, `word_1`, `word_2`, `word_3`, `word_4`, and `word_5`
# MAGIC         - where `word_1` would correspond to the most frequent word in the article, `word_2` the second most frequent word, ...
# MAGIC
# MAGIC Even though the tasks ask for data frame answers, RDDs or Datasets can be helpful. However, their use is optional, and all the tasks can be completed by only using data frames.

# COMMAND ----------

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("WikipediaXMLAnalysis").getOrCreate()

paths = [
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0001.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0002.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0003.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0004.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0005.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0006.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0007.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0008.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0009.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0010.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0011.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0012.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0013.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0014.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0015.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0016.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0017.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0018.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0019.xml",
    "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/wikipedia-20251104-0020.xml",
]

wikiDF = (
    spark.read.format("xml")
    .option("rowTag", "page")
    .load(paths)
    .select(
        col("title").alias("title"),
        col("revision.text._VALUE").alias("text"),
        col("revision.timestamp").alias("timestamp"),
    )
)

punct_to_remove = ".,;:!?()[]{}"
wordsDF = wikiDF.select(
    "title",
    "timestamp",
    "text",
    explode(split(regexp_replace(col("text"), "\n", " "), " ")).alias("raw_word"),
).select(
    col("title"),
    col("timestamp"),
    col("text"),
    trim(lower(translate(col("raw_word"), punct_to_remove, ""))).alias("word"),
)

valid_wordsDF = wordsDF.where(
    (col("word") != "")
    & (col("word").rlike("^[a-z]+$"))
    & (length(col("word")) >= 5)
    & (
        ~col("word").isin(
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
            "summer",
            "autumn",
            "winter",
            "spring",
        )
    )
)

articlesCount = wikiDF.select("title").distinct().count()

tenMostFrequentWordsDF: DataFrame = (
    valid_wordsDF.groupBy("word")
    .agg(spark_count(lit(1)).alias("total_count"))
    .orderBy(col("total_count").desc(), col("word").asc())
    .limit(10)
)

# COMMAND ----------

print("Top 10 most frequent words across all articles:")
tenMostFrequentWordsDF.show(truncate=False)

# COMMAND ----------

softwareCountPerArticleDF = (
    valid_wordsDF.where(col("word") == "software")
    .groupBy("title")
    .agg(spark_count(lit(1)).alias("count_software"))
    .where(col("count_software") > 5)
    .orderBy(col("title").asc())
)

softwareArticles: list[str] = [
    row["title"] for row in softwareCountPerArticleDF.collect()
]

# COMMAND ----------

print("Articles in alphabetical order where the word 'software' appears more than 5 times:")
for title in softwareArticles:
    print(f" - {title}")

# COMMAND ----------

wordArticleFreqDF = (
    valid_wordsDF.select("word", "title")
    .distinct()
    .groupBy("word")
    .agg(countDistinct("title").alias("article_count"))
    .withColumn("word_length", length(col("word")))
)

N = articlesCount
thresholds = {
    "10": int((N * 0.10 + 0.999999) // 1) if N > 0 else 0,
    "25": int((N * 0.25 + 0.999999) // 1) if N > 0 else 0,
    "50": int((N * 0.50 + 0.999999) // 1) if N > 0 else 0,
    "75": int((N * 0.75 + 0.999999) // 1) if N > 0 else 0,
    "90": int((N * 0.90 + 0.999999) // 1) if N > 0 else 0,
}


def top10_for_threshold(min_articles):
    return (
        wordArticleFreqDF.where(col("article_count") >= lit(min_articles))
        .orderBy(col("word_length").desc(), col("word").asc())
        .select(col("word").alias("word"), "word_length")
        .limit(10)
        .withColumn(
            "rank",
            row_number().over(
                Window.orderBy(col("word_length").desc(), col("word").asc())
            ),
        )
        .select("rank", "word")
    )


top10_10 = top10_for_threshold(thresholds["10"]).withColumnRenamed("word", "word_10")
top10_25 = top10_for_threshold(thresholds["25"]).withColumnRenamed("word", "word_25")
top10_50 = top10_for_threshold(thresholds["50"]).withColumnRenamed("word", "word_50")
top10_75 = top10_for_threshold(thresholds["75"]).withColumnRenamed("word", "word_75")
top10_90 = top10_for_threshold(thresholds["90"]).withColumnRenamed("word", "word_90")

longestWordsDF: DataFrame = (
    top10_10.join(top10_25, on="rank", how="outer")
    .join(top10_50, on="rank", how="outer")
    .join(top10_75, on="rank", how="outer")
    .join(top10_90, on="rank", how="outer")
    .orderBy(col("rank").asc())
)

# COMMAND ----------

print("The longest words appearing in at least 10%, 25%, 50%, 75, and 90% of the articles:")
longestWordsDF.show(truncate=False)

# COMMAND ----------

articlesBeforeOct2025DF = wikiDF.select(
    col("title"), to_timestamp(col("timestamp")).alias("ts"), col("text")
).where(col("ts") < to_timestamp(lit("2025-10-01T00:00:00Z")))

valid_words_preOctDF = (
    valid_wordsDF.alias("vw")
    .join(
        articlesBeforeOct2025DF.select(
            col("title").alias("t_title"),
            col("ts").alias("t_ts"),
            col("text").alias("t_text"),
        ),
        valid_wordsDF.title == col("t_title"),
        "inner",
    )
    .select(
        col("t_title").alias("title"),
        col("t_ts").alias("ts"),
        col("t_text").alias("text"),
        col("word"),
    )
)

wordCountsPerArticleDF = valid_words_preOctDF.groupBy(
    "title", "ts", "text", "word"
).agg(spark_count(lit(1)).alias("cnt"))

w = Window.partitionBy("title").orderBy(col("cnt").desc(), col("word").asc())
rankedWordsDF = wordCountsPerArticleDF.withColumn("rn", row_number().over(w)).where(
    col("rn") <= 5
)

pivotDF = (
    rankedWordsDF.select("title", "ts", "word", "rn")
    .groupBy("title", "ts")
    .pivot("rn", [1, 2, 3, 4, 5])
    .agg(expr("first(word)"))
    .withColumnRenamed("1", "word_1")
    .withColumnRenamed("2", "word_2")
    .withColumnRenamed("3", "word_3")
    .withColumnRenamed("4", "word_4")
    .withColumnRenamed("5", "word_5")
)

articleCharsDF = articlesBeforeOct2025DF.select(
    col("title"),
    col("ts"),
    length(col("text")).alias("characters"),
    date_format(col("ts"), "yyyy-MM-dd").alias("date"),
)

frequentWordsDF: DataFrame = (
    pivotDF.join(articleCharsDF, on=["title", "ts"], how="inner")
    .select(
        "title", "date", "characters", "word_1", "word_2", "word_3", "word_4", "word_5"
    )
    .orderBy(col("date").asc(), col("title").asc())
)

# COMMAND ----------

print("Top 5 most frequent words per article (excluding forbidden words) in articles last updated before October 2025:")
frequentWordsDF.show(truncate=False)
