package org.example
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_set, desc, first}

import java.io.IOException


/**
 * @author Frederico Aleixo
 */
object App {
  //Constants
  val UserReviewsPath = "google-play-store-apps/googleplaystore_user_reviews.csv"
  val PlayStorePath = "google-play-store-apps/googleplaystore.csv"
  val OutputSrcPath = "output/intermediate"
  val Part2OutputDstPath = "output/best_apps.csv"


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
        "LEFT JOIN nonNanAvgs ON data.App = nonNanAvgs.App")

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
    val df = spark.read.option("header", "true").csv(PlayStorePath)

    df.createOrReplaceTempView("data")

    spark.sql("" +
      "Select App, Rating " +
      "FROM data " +
      "WHERE Rating >= 4.0 " +
      "ORDER BY Rating DESC")

    df.write.option("header", true)
            .option("delimiter", "&")
            .mode("overwrite")
            .csv(OutputSrcPath)
    merge(OutputSrcPath, Part2OutputDstPath)
  }

  def part3Func(spark: SparkSession): DataFrame = {
    val df = spark.read.option("header", "true").csv(PlayStorePath)

    df.createOrReplaceTempView("data")

    //Dataframe with the values cleaned
    //TODO: Invalid entries that have misplaced values in some columns would require additional clean up...
    val clean_df = spark.sql("" +
      "Select App, " +
      "       Category as Categories," +
      "       CASE" +
      "         WHEN Rating = 'NaN' then null" +
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
    val unmerged_df = clean_df.withColumn("Genres",functions.split(df("Genres"),";"))

    //Merge categories of apps with the same name, keeping the review column with the highest reviews
    val merged_categories_df = unmerged_df.groupBy("App")
      .agg(
        collect_set("Categories").alias("Category"),
        functions.max("Reviews").alias("Reviews"),
      )

    //Create the final dataframe by joining the two dataframes using app name and reviews as keys
    unmerged_df.createOrReplaceTempView("unmerged")
    merged_categories_df.createOrReplaceTempView("merged")
    spark.sql("" +
      "SELECT u.App," +
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

  def main(args : Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val df_1 = part1Func(spark, false)
    //df_1.show()
    //part2Func(spark)
    val df_3 = part3Func(spark)
    df_3.show()
  }
}
