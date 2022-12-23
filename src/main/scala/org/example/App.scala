package org.example
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * @author Frederico Aleixo
 */
object App {

  def part1Func(spark: SparkSession): DataFrame = {
    val pathIn = "google-play-store-apps/googleplaystore_user_reviews.csv"
    val df = spark.read.option("header","true").csv(pathIn)

    df.createOrReplaceTempView("data")

    //Assuming that 'nan' values for Sentiment_Polarity are not included in the computation of AVG,
    //to prevent skewing the data in the wrong direction
    spark.sql("" +
      "SELECT App, IFNULL(AVG(Sentiment_Polarity),0) as Average_Sentiment_Polarity " +
      "FROM data " +
      "WHERE Sentiment_Polarity IS NOT NULL AND Sentiment_Polarity != 'nan' " +
      "GROUP BY App ")
  }
  def main(args : Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    part1Func(spark).show()
  }
}
