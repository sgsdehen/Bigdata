package com.lancer.scala

import com.lancer.scala.model.Person
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector

import java.util.Properties

object Test02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val p = new Properties()

    p.load(this.getClass.getClassLoader.getResourceAsStream("producer.properties"))

    val source = env.socketTextStream("bigdata01", 9999)

    import org.apache.flink.api.scala._

    val outputTag = OutputTag[String]("adult")

    val outputDataStream: DataStream[Person] = source
      .filter(
        (line: String) => {
          if (!"".equals(line)) {
            val splits = line.split(",")
            splits.length == 3 && (splits(2).trim.toInt >= 0 && splits(2).trim.toInt <= 100)
          } else {
            false
          }
        })
      .process[Person](
        new ProcessFunction[String, Person] {
          override def processElement(value: String, ctx: ProcessFunction[String, Person]#Context, out: Collector[Person]): Unit = {
            val splits = value.split(",")
            val name = splits(0).trim
            val gender = splits(1).trim
            val age = splits(2).trim.toInt
            if (age >= 18) out.collect(Person(name, gender, age)) else ctx.output[String](outputTag, value)
          }
        }
      )

    outputDataStream
      .print()

    outputDataStream
      .getSideOutput[String](outputTag)
      .addSink(new FlinkKafkaProducer[String]("flinkTest", new SimpleStringSchema(), p)
      )
    env.execute(this.getClass.getSimpleName)
  }
}
