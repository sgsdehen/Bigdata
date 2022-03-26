package com.lancer.doit.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import utils.SparkEnvUtils

/**
 * @Author lancer
 * @Date 2022/2/18 8:11 下午
 * @Description
 */
object E01_ConsumeDataFromKafka {
  def main(args: Array[String]): Unit = {
    val ssc = SparkEnvUtils.getEnv("streaming", appName = "SparkStreaming_DirectAPI020", streamingInterval = Seconds(2)).asInstanceOf[StreamingContext]

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bigdata01:9092,bigdata02:9092,bigdata03:9092,bigdata04:9092,bigdata05:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test_group",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Seq("test"), kafkaParams)
    )

    kafkaDStream
      .map(_.value())
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
