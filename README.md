# Elon Musk Tweet Keyword Analyzer (Spark + Scala)

This project analyzes a CSV file containing a large collection of tweets posted by **Elon Musk**.  
It extracts statistics based on user-entered keywords, including keyword frequency over time, tweet coverage percentages, and text length statistics.

> [!IMPORTANT]  
> This is used for studying, learning, and practicing purposes.

> [!NOTE]  
> This activity/assignment is exclusive for `Big Data Engineering` course at `An-Najah National University`.

---

## Features
The application receives a comma-separated list of keywords and prints the following:

1. **Distribution of keywords over time (day-wise)**  
  
2. **Percentage of tweets that contain at least one keyword**

3. **Percentage of tweets that contain exactly two of the entered keywords**

4. **Average and standard deviation of tweet length (in words)**

All computations are implemented using **RDD transformations** - no loops, no Spark SQL.

---

## Stack
- Java 8 (1.8.0_451)
- Spark 3.5.7
- Scala 2.12.18
- SBT

---

## How to Run

### 1. Run using SBT
```bash
sbt run
```
### 2. Specify a custom CSV path
```bash
sbt "run /path/to/elonmusk_tweets.csv"
```

>If no CSV path is provided, the app will automatically use:
>data/elonmusk_tweets.csv

### 3. Run with a custom CSV path, with the keywords to search for
```bash
sbt "run data/elonmusk_tweets.csv tesla,spacex,ai"
```
>If no keywords is provided, the app will automatically prompt the user the enter the keywords when the app starts.