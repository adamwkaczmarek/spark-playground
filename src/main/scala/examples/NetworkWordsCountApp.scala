package examples

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming._

import java.io.{File, FileWriter}


object NetworkWordsCountApp extends App {
  private[this] val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  private[this] val ssc = new StreamingContext(conf, Seconds(5))
  private[this] val port = 9999
  private[this] val host = "localhost"

  //POSTGRES DB
  private[this] val driver = "org.postgresql.Driver"
  private[this] val url = "jdbc:postgresql://localhost:5432/sparkexamples"
  private[this] val user = "docker"
  private[this] val password = "docker"
  private[this] val dbName = "public.networkwordscount"

  case class Entry(word: String)

  object Entry {
    def apply(word: String): Entry =
      new Entry(word.toLowerCase().replaceAll("[^a-zA-Z]+", ""))
  }

  ssc.socketTextStream(host, port)
    .flatMap(_.split(" "))
    .foreachRDD((rdd: RDD[String], time: Time) => {
      val wordCountsDataFrame: DataFrame = count(rdd, time)
      saveToDb(wordCountsDataFrame)
    }
    )
  ssc.start() // Start the computation
  ssc.awaitTermination() // Wait for the computation to terminate

  private def saveToDb(wordCountsDataFrame: DataFrame) = {
    wordCountsDataFrame.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", dbName)
      .save()
  }

  private def count(rdd: RDD[String], time: Time): DataFrame = {
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    import spark.implicits._

    val wordsDataFrame = rdd.map(w => Entry(w)).toDF()
    wordsDataFrame.createOrReplaceTempView("words")
    val wordCountsDataFrame =
      spark.sql("select word, count(*) as total from words group by word")
    println(s"========= $time =========")
    wordCountsDataFrame.show()
    wordCountsDataFrame
  }
}
