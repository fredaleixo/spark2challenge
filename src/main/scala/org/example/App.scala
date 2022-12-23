package org.example
import org.apache.spark.sql.SparkSession


/**
 * @author Frederico Aleixo
 */
object App {

  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))

    val spark: SparkSession = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val pathIn = "google-play-store-apps/googleplaystore.csv"
    spark.read.csv(pathIn).show()
  }

}
