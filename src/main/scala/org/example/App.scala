package org.example
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.functions.{avg, collect_set, count, explode, when}
import java.io.IOException

/**
 * @author Frederico Aleixo
 */
object App {
  //Constants
  val UserReviewsPath = "google-play-store-apps/googleplaystore_user_reviews.csv"
  val PlayStorePath = "google-play-store-apps/googleplaystore.csv"
  val Part2OutputSrcPath = "output/intermediate2"
  val Part2OutputDstPath = "output/best_apps.csv"
  val Part4OutputSrcPath = "output/intermediate4"
  val Part4OutputDstPath = "output/googleplaystore_cleaned.parquet"
  val Part5OutputSrcPath = "output/intermediate5"
  val Part5OutputDstPath = "output/googleplaystore_metrics.parquet"


  //Auxiliary methods
  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 deleteSource: Boolean, conf: Configuration
               ): Boolean = {

    //Disabling checksums so that the produced result is only a single file
    srcFS.setWriteChecksum(false)
    srcFS.setVerifyChecksum(false)

    if (dstFS.exists(dstFile)) {
      throw new IOException(s"Target $dstFile already exists")
    }

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory) {

      val outputFile = dstFS.create(dstFile)
      try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile =>
              val inputFile = srcFS.open(status.getPath)
              try {
                IOUtils.copyBytes(inputFile, outputFile, conf, false)
              }
              finally {
                inputFile.close()
              }
          }
      } finally {
        outputFile.close()
      }

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }

  def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig)
    // the "true" setting deletes the source files once they are merged into the new output
  }


  //Challenge methods
  def part1Func(spark: SparkSession, includeNans: Boolean): DataFrame = {
    val df = spark.read.option("header","true").csv(UserReviewsPath)

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
        "LEFT JOIN nonNanAvgs ON data.App = nonNanAvgs.App ")

    } else {
      //Assuming that 'nan' values for Sentiment_Polarity are included and considered to have a value of nanValue
      val nanValue = 0
      spark.sql("" +
        "WITH nanToNull as " +
        " (SELECT App, NULLIF(Sentiment_Polarity, 'nan') as Sentiment_Polarity" +
        "  FROM data) " +
        "SELECT App, IFNULL(AVG(COALESCE(Sentiment_Polarity," + nanValue + ")),0) as Average_Sentiment_Polarity " +
        "FROM nanToNull " +
        "GROUP BY App ")
    }
  }

  def part2Func(spark: SparkSession): Unit = {
    var df = spark.read.option("header", "true").csv(PlayStorePath)

    df.createOrReplaceTempView("data")

    df = spark.sql("" +
      "Select App, Rating " +
      "FROM data " +
      "WHERE Rating != 'NaN' AND Rating >= 4.0 AND Rating <= 5.0 " +
      "ORDER BY Rating DESC")

    df.write.option("header", true)
            .option("delimiter", "&")
            .mode("overwrite")
            .csv(Part2OutputSrcPath)
    merge(Part2OutputSrcPath, Part2OutputDstPath)
  }

  def part3Func(spark: SparkSession): DataFrame = {
    val df = spark.read.option("header", "true").csv(PlayStorePath)

    df.createOrReplaceTempView("data")

    //Dataframe with the values cleaned
    //(Invalid entries that have misplaced values in some columns would require additional clean up)
    val cleanDf = spark.sql("" +
      "Select App, " +
      "       Category as Categories," +
      "       CASE" +
      "         WHEN Rating = 'NaN' OR Rating > 5.0 then null" +
      "         ELSE Rating" +
      "       END AS Rating, " +
      "       Reviews, " +
      "       CASE " +
      "         WHEN Size LIKE '%M' then CAST(SUBSTRING(Size, 1, (LENGTH(Size)-1)) AS DOUBLE)" +
      "         ELSE null" +
      "       END AS Size," +
      "       Installs," +
      "       Type," +
      "       CAST(Price as DECIMAL(9,2)) * 0.9 as Price," +
      "       `Content Rating` as Content_Rating," +
      "       Genres," +
      "       TO_TIMESTAMP(`Last Updated`, 'MMMM d, yyyy') as Last_Updated," +
      "       `Current Ver` as Current_Version," +
      "       `Android Ver` as Minimum_Android_Version " +
      "FROM data d ")

    //Turn Genres from string to array of strings, with the categories still unmerged
    val unmergedDf = cleanDf.withColumn("Genres",functions.split(df("Genres"),";"))

    //Merge categories of apps with the same name, keeping the review column with the highest reviews
    val mergedCategoriesDf = unmergedDf.groupBy("App")
      .agg(
        collect_set("Categories").alias("Category"),
        functions.max("Reviews").alias("Reviews"),
      )

    //Create the final dataframe by joining the two dataframes using app name and reviews as keys
    unmergedDf.createOrReplaceTempView("unmerged")
    mergedCategoriesDf.createOrReplaceTempView("merged")
    spark.sql("" +
      "SELECT DISTINCT u.App," +
      "       m.Category as Categories," +
      "       u.Rating," +
      "       u.Reviews," +
      "       u.size," +
      "       u.installs," +
      "       u.type," +
      "       u.price," +
      "       u.Content_Rating," +
      "       u.Genres," +
      "       u.Last_Updated," +
      "       u.Current_Version," +
      "       u.Minimum_Android_Version " +
      "FROM merged m " +
      "INNER JOIN unmerged u ON m.App = u.App AND m.Reviews = u.Reviews ")
  }

  def part4Func(df1: DataFrame, df3: DataFrame): Unit = {
    val innerJoinDf = df3.join(df1, df3("App") === df1("App"),"inner")
                            .select(df3("*"),df1("Average_Sentiment_Polarity"))

    innerJoinDf.write.option("header", true)
      .mode("overwrite")
      .option("compression","GZIP")
      .parquet(Part4OutputSrcPath)
    merge(Part4OutputSrcPath, Part4OutputDstPath)
  }

  def part5Func(spark: SparkSession, df3: DataFrame): Unit = {
    val reviewsDf = spark.read.option("header","true").csv(UserReviewsPath)

    val joinedDf = df3.join(reviewsDf,df3("App") === reviewsDf("App"), "inner")
                      .select(df3("*"),reviewsDf("Sentiment_Polarity"))

    val cleanJoinedDf = joinedDf.withColumn("Sentiment_Polarity",
                      when(joinedDf("Sentiment_Polarity") === "nan", "null")
                      .otherwise(joinedDf("Sentiment_Polarity")))

    val explodedDf = cleanJoinedDf.withColumn("Genre", explode(df3("Genres")))

    val df4 = explodedDf.groupBy("Genre").agg(
      count("App").alias("Count"),
      avg("Rating").alias("Average_Rating"),
      avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    df4.write.option("header", true)
      .mode("overwrite")
      .option("compression", "GZIP")
      .parquet(Part5OutputSrcPath)
    merge(Part5OutputSrcPath, Part5OutputDstPath)
  }

  def main(args : Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val df1 = part1Func(spark, false)
    df1.show()
    part2Func(spark)
    val df3 = part3Func(spark)
    df3.show()
    part4Func(df1, df3)
    part5Func(spark, df3)
  }
}
