# COMP.CS.320 - Group assignment - Advanced task 2

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
    - This notebook is for **advanced task 2**
- Moodle submission for the group

---

## Advanced Task 2 - Wikipedia articles

The folder `assignment/wikipedia` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2025-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2025gen2/path/shared/etag/%220x8DE01A3A1A66C90%22/defaultId//publicAccessVal/None) contains a number of the longest Wikipedia articles ([https://en.wikipedia.org/w/index.php?title=Special:LongPages&limit=500&offset=0](https://en.wikipedia.org/w/index.php?title=Special:LongPages&limit=500&offset=0)).<br>
The most recent versions of the articles were extracted in XML format using Wikipedia's export tool: [https://en.wikipedia.org/wiki/Special:Export](https://en.wikipedia.org/wiki/Special:Export)

Spark has support for importing XML files directly to DataFrames, [https://spark.apache.org/docs/latest/sql-data-sources-xml.html](https://spark.apache.org/docs/latest/sql-data-sources-xml.html).

#### Definition for a word to be considered in this task

A word is to be considered (and included in the counts) in this task if<br>

- when the following punctuation characters are removed: '`.`', '`,`', '`;`', '`:`', '`!`', '`?`', '`(`', '`)`', '`[`', '`]`', '`{`', '`}`',<br>
- and all letters have been changed to lower case, i.e., `A` -> `a`, ...

the word fulfils the following conditions:

- the word contains only letters in the English alphabet: '`a`', ..., '`z`'
- the word is at least 5 letters long
- the word is not the English word for a specific month:<br>
    `january`, `february`, `march`, `april`, `may`, `june`, `july`, `august`, `september`, `october`, `november`, `december`
- the word in not the English word for a specific season: `summer`, `autumn`, `winter`, `spring`

For example, words `(These` and `country,` would be valid words to consider with these rules (as `these` and `country`).

In this task, you can assume that each line in an article is separated by the new line character, '`\n`'.<br>
And that each word is separated by a whitespace character, '` `'.

#### The tasks

Load the content of the Wikipedia articles, and find the answers to the following questions using the presented criteria for a word:

- What are the 10 most frequent words across all included articles?
    - Give the answer as a data frame with columns `word` and `total_count`.
- In which articles does the word `software` appear more than 5 times?
    - Give the answer as a list of articles titles.
- What are the 10 longest words that appear in at least 10% of the articles?
    - And the same for at least 25%, 50%, 75%, and 90% of the articles.
    - Words that have the same length should be ranked alphabetically.
    - Give the answer as a data frame with columns `rank`, `word_10`, `word_25`, `word_50`, `word_75`, and `word_90`.
- What are the 5 most frequent words per article in the articles last updated before October 2025?
    - In the answer, include the title and the date for the article, as well as the full character count for the article.
    - Give the answer as a data frame with columns `title`, `date`, `characters`, `word_1`, `word_2`, `word_3`, `word_4`, and `word_5`
        - where `word_1` would correspond to the most frequent word in the article, `word_2` the second most frequent word, ...

Even though the tasks ask for data frame answers, RDDs or Datasets can be helpful. However, their use is optional, and all the tasks can be completed by only using data frames.
