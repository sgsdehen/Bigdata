package com.lancer.scala

import org.apache.flink.streaming.api.scala._

object StreamingWordCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[String] = env.socketTextStream("bigdata01", 9999)

    source
      .flatMap(line => line.toLowerCase.split(" ")).setParallelism(2)
      .filter(_.nonEmpty).setParallelism(2)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()

    env.execute("WordCount")
  }
}
