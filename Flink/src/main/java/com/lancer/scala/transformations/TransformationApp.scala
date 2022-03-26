package com.lancer.scala.transformations

import org.apache.commons.lang3.math.NumberUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util
import scala.util.Random

object TransformationApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    transform_outputTag(env)

    env.execute("TransformationApp")
  }

  def transform_outputTag(env: StreamExecutionEnvironment): Unit = {
    val srcDataStream: DataStream[Access] = env
      .readTextFile("./data/access.log")
      // transformation
      .map(
        line => {
          val splits = line.split(",");
          val time = splits(0).trim.toLong
          val domain = splits(1).trim
          val traffic = splits(2).trim.toDouble
          Access(time, domain, traffic)
        })

    val outputTag: OutputTag[Access] = OutputTag[Access]("isBad")

    val resultStream: DataStream[Access] = srcDataStream
      .process[Access](new ProcessFunction[Access, Access] {
        override def processElement(value: Access, ctx: ProcessFunction[Access, Access]#Context, out: Collector[Access]): Unit = {
          val isGood: Boolean = value.traffic > 2000
          if (isGood) out.collect(value) else ctx.output(outputTag, value)
        }
      })

    resultStream.map[(String, Access)](
      (t: Access) => {
        ("hello word----------->", t)
      }
    )
    .print("isGood")

    resultStream.getSideOutput[Access](outputTag).print("isBad")
  }

  /*def transform_split_select(env: StreamExecutionEnvironment): Unit = {
    val splitStream: SplitStream[Access] = env
      .readTextFile("./data/access.log")
      // transformation
      .map(
        line => {
          val splits = line.split(",");
          val time = splits(0).trim.toLong
          val domain = splits(1).trim
          val traffic = splits(2).trim.toDouble
          Access(time, domain, traffic)
        })
      .split(access => {
        val isGood = access.traffic > 2000
        if (isGood) {
          Seq("isGood")
        } else {
          Seq("isBad")
        }
      })

    splitStream.select("isGood").print("isGood")

    splitStream.select("isBad").print("isBad")
  }*/

  // 在实际生产环境中应该尽量避免在一个无限流上使用 Aggregations
  def transform_max_maxBy(env: StreamExecutionEnvironment): Unit = {
    env
      .readTextFile("./data/access.log")

      // transformation
      .map(
        line => {
          val splits = line.split(",");
          val time = splits(0).trim.toLong
          val domain = splits(1).trim
          val traffic = splits(2).trim.toDouble
          Access(time, domain, traffic)
        })
      /*.keyBy[(Long, String)]((access: Access) => (access.time, access.domain))
      .max("traffic")*/
      .keyBy("time")
      .maxBy("traffic")
      /*.keyBy(value => value.time)
      .max("traffic")*/
      // sink
      .print();
  }

  def transform_coFlatMap(env: StreamExecutionEnvironment): Unit = {
    val source1: DataStream[String] = env.fromElements[String]("a b c", "a b c")
    val source2: DataStream[String] = env.fromElements[String]("d,e,f", "d,e,f")

    val connectStream: ConnectedStreams[String, String] = source1.connect(source2)

    val flatMapStream: DataStream[String] = connectStream.flatMap((str, ctx) => str.split(" ").foreach(ctx.collect), (str, ctx) => str.split(",").foreach(ctx.collect))

    flatMapStream.print()
  }

  def transform_coMap(env: StreamExecutionEnvironment): Unit = {
    val source1: DataStream[String] = env.socketTextStream("bigdata01", 9998)
    val source2: DataStream[Int] = env.socketTextStream("bigdata01", 9999)
      .filter(NumberUtils.isNumber(_))
      .map[Int]((num: String) => num.toInt)

    val connectStream: ConnectedStreams[String, Int] = source1.connect(source2)

    val mapStream: DataStream[String] = connectStream.map[String]((line: String) => line.toUpperCase, (t: Int) => t * 10 + "")

    mapStream.print()
  }

  def transform_connect(env: StreamExecutionEnvironment): Unit = {
    val source: RichSourceFunction[Access] = new RichSourceFunction[Access] {

      private var running: Boolean = true

      override def open(parameters: Configuration): Unit = print("~~~~~open~~~~~")

      override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
        val domains = Array[String]("lancer.com", "d.com", "b.com")
        while (running) {
          for (_ <- 1 to 10)
            ctx.collect(Access(1234567.toLong, domains(Random.nextInt(domains.length)), Random.nextDouble() + 1000))
          Thread.sleep(5000)
        }
      }

      override def cancel(): Unit = running = false
    }

    val source1: DataStream[Access] = env.addSource(source)
    val source2: DataStream[Access] = env.addSource(source)

    val newSource2: DataStream[(String, Access)] = source2.map(("lancer", _))

    val connectStream: ConnectedStreams[Access, (String, Access)] = source1.connect(newSource2)

    // 当transform算子参数列表的每个位置的匿名函数的参数列表(个数或类型)不一样时，需要指定类型
    val mapStream: DataStream[String] = connectStream.map[String]((value: Access) => value.toString, (t: (String, Access)) => s"${t._1} ====> ${t._2}")

    // 传入两个参数，一个
    val fun1: (String, Access) => String = (k, v) => s"$k ===> $v"
    // 传入一个参数，元组
    val fun2: ((String, Access)) => String = t => s"${t._1} ===> ${t._2}"

    mapStream.print()
  }

  def transform_union(env: StreamExecutionEnvironment): Unit = {
    val source1 = env.socketTextStream("bigdata01", 9998)
    val source2 = env.socketTextStream("bigdata01", 9999)

    val unionStream = source1.union(source2)
    // source1.union(source1).print()
    unionStream.print()
  }

  def transform_reduce(env: StreamExecutionEnvironment): Unit = {
    env.socketTextStream("bigdata01", 9999)
      .flatMap(line => line.split(" "))
      .map((_, 1))
      .keyBy(t => t._1)
      .reduce((pre: (String, Int), next: (String, Int)) => (pre._1, pre._2 + next._2))
      .print
  }

  def transform_keyBy(env: StreamExecutionEnvironment): Unit = {
    env.readTextFile("./data/access.log")
      .map(
        line => {
          val splits = line.split(",")
          val time = splits(0).trim.toLong
          val domain = splits(1).trim
          val traffic = splits(2).trim.toDouble
          Access(time, domain, traffic)
        }
      )
      .keyBy(value => value.domain)
      .sum(2)
      .print()
    /*.keyBy("domain")
    .sum("traffic")
    .print()*/
    /*.keyBy(1)
    .sum(2)
    .print()*/
  }

  def transform_flatMap(env: StreamExecutionEnvironment): Unit = {
    env.socketTextStream("bigdata01", 9999)
      .flatMap(line => line.split(" "))
      .filter(!"spark".equals(_))
      .print()
  }

  def transform_filter(env: StreamExecutionEnvironment): Unit = {
    env.readTextFile("./data/access.log")
      .map {
        line => {
          val splits = line.split(",")
          Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toDouble)
        }
      }
      .filter(_.traffic > 4000)
      .print
  }

  def transform_richMap(env: StreamExecutionEnvironment): Unit = {
    class PKMapFunction extends RichMapFunction[String, Access] {
      override def open(parameters: Configuration): Unit = {
        println("~~~~~~open~~~~~~")
      }

      override def close(): Unit = super.close()

      override def map(value: String): Access = {
        val splits: Array[String] = value.split(",")
        Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toDouble)
      }
    }

    val source = env.readTextFile("./data/access.log")
    val mapStream = source.map(new PKMapFunction)
    mapStream.print()
  }

  def transform_map(env: StreamExecutionEnvironment): Unit = {
    val source = env.readTextFile("./data/access.log")

    val mapStream = source
      .map(line => {
        val splits = line.split(",")
        Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toDouble)
      })

    mapStream.print
  }
}

case class Access(time: Long, domain: String, traffic: Double)