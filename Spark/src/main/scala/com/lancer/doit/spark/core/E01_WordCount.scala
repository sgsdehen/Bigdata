package com.lancer.doit.spark.core

import org.apache.spark.SparkContext
import utils.SparkEnvUtils

object E01_WordCount {
  def main(args: Array[String]): Unit = {
    val sc = SparkEnvUtils.getEnv("context", this.getClass.getSimpleName).asInstanceOf[SparkContext]

    var count = 0 // 存在闭包引用

    sc.textFile("Spark-core/data/wordcount/input/a.txt")
      .filter(_.nonEmpty)
      .flatMap(line => line.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .foreach(x => {
        count += 1
        println(count, x)
      })
    sc.stop()
  }
}
