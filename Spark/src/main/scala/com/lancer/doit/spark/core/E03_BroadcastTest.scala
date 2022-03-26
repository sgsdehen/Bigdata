package com.lancer.doit.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import utils.SparkEnvUtils

import scala.collection.immutable.HashMap


object E03_BroadcastTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkEnvUtils.getEnv("context", "broadcast", logLevel = "error").asInstanceOf[SparkContext]

    val source = sc.textFile("Spark-core/data/stu/input/a.txt")

    val mp = HashMap[Int, String](1 -> "zhangsan", 2 -> "lisi")

    val bc: Broadcast[HashMap[Int, String]] = sc.broadcast(mp)

    val resultRDD = source
      .map(line => {
        val arr = line.split(",")
        (arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toInt, arr(4))
      })
      .mapPartitions(iter => {
        val smallTable = bc.value // 广播过来的小表数据

        iter.map(tp => { // 两表进行关联
          val id = tp._1
          val name = smallTable.getOrElse(id, "未知")
          (id, name, tp._2, tp._3, tp._4, tp._5)
        })
      })

    resultRDD.foreach(println)

    sc.stop()
  }

}
