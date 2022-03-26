package com.lancer.doit.spark.structured

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.SparkEnvUtils

/**
 * @Author lancer
 * @Date 2022/2/11 10:31 下午
 * @Description
 */
object E01_WordCountWithStructuredStreaming {

  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args: Array[String]): Unit = {
    val spark = SparkEnvUtils.getEnv("session", logLevel = "error").asInstanceOf[SparkSession]

    import spark.implicits._

    val lines = spark
      .readStream
      .format("socket") // 支持file source、kafka source、socket source
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words: Dataset[String] = lines.as[String].flatMap(_.split(" ")).as("value")

    val wordCounts = words.groupBy("value").count()


    // 将数据存与内存的表中
    wordCounts
      .writeStream
      .queryName("ad_pv_tb")
      .outputMode(OutputMode.Complete()) // 有聚合计算使用complete，没有使用append
      .option("checkpointLocation", "hdfs://structured_streaming/checkpoint/dir") // 设置检查点路径
      .format("memory") // 支持file sink、foreach sink、console sink、memory sink
      .start()

    spark.sql("select * from ad_pv_tb").show()

    // 将数据存入外部存储系统
    wordCounts
      .writeStream
      .trigger(ProcessingTime("10 seconds"))
      .outputMode("complete") // append
      .format("console")
      .start()
      .awaitTermination()
  }
}
