package com.elon.analyzer

import org.apache.spark.{SparkConf, SparkContext}
import scala.math.sqrt
import scala.io.StdIn

object Main {
  def main(args: Array[String]): Unit = {

    // Default file path if none provided
    val csvPath =
      if (args.length >= 1 && args(0).trim.nonEmpty) args(0)
      else "data/elonmusk_tweets.csv"

    // Ask user for keywords if not passed as args
    val keywordsInput =
      if (args.length >= 2 && args(1).trim.nonEmpty) args(1)
      else {
        println("Enter comma-separated keywords to analyze:")
        StdIn.readLine()
      }

    val keywords = keywordsInput.split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toList

    if (keywords.isEmpty) {
      println("Error: No keywords provided. Please enter at least one keyword.")
      sys.exit(1)
    }

    println(s"\nUsing dataset: $csvPath")
    println(s"Analyzing keywords: ${keywords.mkString(", ")}")

    val conf = new SparkConf()
      .setAppName("Elon Tweet Keyword Analyzer")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Load CSV
    val data = sc.textFile(csvPath)

    // Remove header automatically
    val header = data.first()
    val tweets = data.filter(_ != header)
      .map(_.split(",", -1))
      .filter(_.length >= 2)
      .map(cols => {
        val date = cols(0).trim
        val text = cols.last.trim.toLowerCase
        (date, text)
      })
      .cache()

    // Daily keyword distribution
    val dailyDistribution = tweets
      .flatMap { case (date, text) =>
        keywords.filter(k => text.contains(k)).map(k => ((k, date), 1))
      }
      .reduceByKey(_ + _)
      .map { case ((keyword, date), count) => (keyword, date, count) }

    // Percentages
    val totalTweets = tweets.count()

    val tweetsWithAtLeastOne = tweets.filter { case (_, text) =>
      keywords.exists(k => text.contains(k))
    }.count()

    val tweetsWithExactlyTwo = tweets.filter { case (_, text) =>
      keywords.count(k => text.contains(k)) == 2
    }.count()

    val percentAtLeastOne = (tweetsWithAtLeastOne.toDouble / totalTweets) * 100
    val percentExactlyTwo = (tweetsWithExactlyTwo.toDouble / totalTweets) * 100

    // Tweet length statistics
    val wordCounts = tweets.map { case (_, text) => text.split("\\s+").length.toDouble }
    val mean = wordCounts.sum() / totalTweets
    val variance = wordCounts.map(w => (w - mean) * (w - mean)).sum() / totalTweets
    val stdDev = sqrt(variance)

    // Print results (plain text for Bash compatibility)
    println("\n--- Daily Keyword Distribution ---")
    dailyDistribution.collect().foreach { case (k, d, c) => println(s"($k, $d, $c)") }

    println(f"\nPercentage with at least one keyword: $percentAtLeastOne%.2f%%")
    println(f"Percentage with exactly two keywords: $percentExactlyTwo%.2f%%")

    println(f"\nAverage tweet length: $mean%.2f words")
    println(f"Standard deviation: $stdDev%.2f words")



    println("\nPress ENTER to stop Spark and exit.......")
    scala.io.StdIn.readLine()

    sc.stop()


  }
}