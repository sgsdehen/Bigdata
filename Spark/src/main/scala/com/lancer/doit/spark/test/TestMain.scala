package com.lancer.doit.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import utils.SparkEnvUtils

/**
 * @Author lancer
 * @Date 2022/2/7 4:29 下午
 * @Description
 */
object TestMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkEnvUtils.getEnv("session", logLevel = "error", conf = conf, enableHiveSupport = true).asInstanceOf[SparkSession]

    /*
    val rows = spark.read.table("dwd.event_app_detail")
      .select("deviceId", "account", "filledAccount", "isNew", "guid", "province", "city", "region", "splitSessionId")
      // .where("isNew = 0")
      .show(100, false)*/

    // spark.read.table("ods.event_app_log").select("account").show(100, truncate = false)

    spark.close()
  }

}

