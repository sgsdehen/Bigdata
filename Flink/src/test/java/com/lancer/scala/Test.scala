package com.lancer.scala

import com.lancer.scala.model.Person
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties
import java.util.concurrent.Future

object Test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val p = new Properties()

    p.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    val source = env.socketTextStream("bigdata01", 9999)

    val outputTag = OutputTag[String]("adult")

    val outputDataStream: DataStream[Person] = source
      .filter(
        (line: String) => {
          if (line.trim.nonEmpty) {
            val splits = line.split(",")
            splits.length == 3 && (splits(2).trim.toInt >= 0 && splits(2).trim.toInt <= 100)
          } else {
            false
          }
        })
      .process[Person](new ProcessFunction[String, Person] {
        override def processElement(value: String, ctx: ProcessFunction[String, Person]#Context, out: Collector[Person]): Unit = {
          val splits = value.split(",")
          val name = splits(0).trim
          val gender = splits(1).trim
          val age = splits(2).trim.toInt
          if (age >= 18) out.collect(Person(name, gender, age)) else ctx.output[String](outputTag, value)
        }
      })

    outputDataStream
      .print()

    outputDataStream
      .getSideOutput[String](outputTag)
      .flatMap[String](
        (value: String, out: Collector[String]) => {
          val producer = new KafkaProducer[String, String](p)
          val record = new ProducerRecord[String, String]("flinkTest", value)
          producer.send(record)
          ()
        }
      )
    env.execute(this.getClass.getSimpleName)
  }
}
