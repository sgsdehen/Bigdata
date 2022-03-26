/*
package com.lancer.scala

import com.lancer.scala.model.Person
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.nio.charset.StandardCharsets
import java.util.Properties

object Test03 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val consumer = new Properties()
    val producer = new Properties()

    consumer.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    producer.load(this.getClass.getClassLoader.getResourceAsStream("producer.properties"))

    import org.apache.flink.api.scala._

    // 如果只需要value
    val source: DataStream[String] = env.addSource[String](new FlinkKafkaConsumer[String]("flinkConsole", new SimpleStringSchema(), consumer))

    /* // 如果需要对kafka里面的kv进行操作
    val source = env.addSource[String](new FlinkKafkaConsumer[String]("flinkConsole", new KafkaDeserializationSchema[String] {
      override def isEndOfStream(t: String): Boolean = false

      override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
        new String(consumerRecord.value(), StandardCharsets.UTF_8)
      }

      override def getProducedType: TypeInformation[String] = {
        // createTypeInformation[String]
        // TypeInformation.of(classOf[String])
        Types.STRING
      }
    }, consumer))*/

    // 解决使用泛型嵌套中，遇到的问题
    // TypeInformation.of(classOf[(String, Int)])
    // TypeInformation.of(new TypeHint[(String, Int)] {})
    // TypeInformation.of(new TypeHint[((String, Int), (String, (Int, String)))] {})

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
        new ProcessFunction[String, Person]() {
          override def processElement(value: String, ctx: ProcessFunction[String, Person]#Context, out: Collector[Person]): Unit = {
            val splits = value.split(",");
            val name = splits(0).trim();
            val gender = splits(1).trim();
            val age = Integer.parseInt(splits(2).trim());
            val person = new Person(name, gender, age);
            if (age >= 18) {
              out.collect(person);
            } else {
              ctx.output(outputTag, value);
            }
          }
        }
      )

    outputDataStream
      .print()

    val resultStream: DataStreamSink[String] = outputDataStream
      .getSideOutput[String](outputTag)
      .addSink(new FlinkKafkaProducer[String]("flinkTest", new MyKafkaSerializationSchema, producer, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
      )
    env.execute(this.getClass.getSimpleName)
  }

  class MyKafkaSerializationSchema extends KafkaSerializationSchema[String] {
    override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord("flinkTest", element.getBytes(StandardCharsets.UTF_8))
  }
}
*/
