package com.lancer.doit.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.SparkEnvUtils

object E00_WordCountWithSparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.sql.shuffle.partitions", "2").set("spark.default.parallelism", "1")
    val spark = SparkEnvUtils.getEnv("session", "wordCount", logLevel = "error", conf = conf).asInstanceOf[SparkSession]

    val df = spark.read.text("file:///Users/lancer/IdeaProjects/Spark/Spark-core/data/wordcount/input/a.txt")

    df.createTempView("df")

    spark.sql(
      """
        |select
        | word,
        | count(1) as num
        |from (
        | select
        |  explode(split(value, '\\s+')) as word
        | from df
        |) t
        |group by word
        |order by num desc
        |
        |""".stripMargin).show(100, truncate = false)

    // DSL
    import spark.implicits._
    import org.apache.spark.sql.functions._
    df.select(explode(split($"value", "\\s+")) as "word")
      .groupBy("word")
      .agg(count($"word") as "num")
      .orderBy($"num".desc)
      .show(100, truncate = false)

    spark.close()
  }

}
