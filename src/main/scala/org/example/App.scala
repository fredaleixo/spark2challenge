package org.example
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * @author Frederico Aleixo
 */
object App {

  def part1Func(spark: SparkSession, includeNans: Boolean): DataFrame = {
    val pathIn = "google-play-store-apps/googleplaystore_user_reviews.csv"
    val df = spark.read.option("header","true").csv(pathIn)

    df.createOrReplaceTempView("data")

    if (!includeNans) {
      //It may be desirable to not include 'nan' values for Sentiment_Polarity in the computation of AVG,
      //to prevent skewing the result in the wrong direction by assuming some number for those values
      spark.sql("" +
        "WITH nonNanAvgs as " +
        " (SELECT App, AVG(Sentiment_Polarity) as Average_Sentiment_Polarity " +
        "  FROM data " +
        "  WHERE Sentiment_Polarity != 'nan' " +
        "  GROUP BY App) " +
        "SELECT DISTINCT data.App, IFNULL(Average_Sentiment_Polarity, 0) as Average_Sentiment_Polarity " +
        "FROM data " +
        "LEFT JOIN nonNanAvgs ON data.App = nonNanAvgs.App " +
        "ORDER BY data.App ASC")

    } else {
      //Assuming that 'nan' values for Sentiment_Polarity are included and considered to have a value of nanValue
      val nanValue = 0
      spark.sql("" +
        "WITH nanToNull as " +
        " (SELECT App, NULLIF(Sentiment_Polarity, 'nan') as Sentiment_Polarity" +
        "  FROM data) " +
        "SELECT App, IFNULL(AVG(COALESCE(Sentiment_Polarity," + nanValue + ")),0) as Average_Sentiment_Polarity " +
        "FROM nanToNull " +
        "GROUP BY App " +
        "ORDER BY App ASC")
    }
  }

  def main(args : Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val df_1 = part1Func(spark, false)
    df_1.show()
  }
}
