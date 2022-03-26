package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkEnvUtils {

  def getEnv(envName: String, appName: String = "spark-task", master: String = "local", logLevel: String = "info", conf: SparkConf = new SparkConf(), enableHiveSupport: Boolean = false, streamingInterval: Duration = Seconds(3)): Logging = {

    // 设置日志输出级别
    Logger.getLogger("org.apache.spark").setLevel {
      logLevel match {
        case "info" => Level.INFO
        case "debug" => Level.DEBUG
        case "warn" => Level.WARN
        case "error" => Level.ERROR
        case _ => throw new RuntimeException("日志级别参数错误！")
      }
    }

    val spark = if (enableHiveSupport) {
      SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .config(conf)
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", "hdfs://bigdata01:9000/user/hive/warehouse")
        .config("hive.metastore.warehouse.dir", "hdfs://bigdata01:9000/user/hive/warehouse")
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .config(conf)
        .getOrCreate()
    }

    envName match {
      case "context" => spark.sparkContext
      case "session" => spark
      case "streaming" => new StreamingContext(spark.sparkContext, streamingInterval)
      case _ => throw new RuntimeException("context --> SparkContext and session --> SparkSession")
    }
  }
}
