package com.lancer.scala

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val source = env.readTextFile("./data/word.txt")
    source
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
