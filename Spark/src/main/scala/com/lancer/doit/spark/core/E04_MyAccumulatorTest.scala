package com.lancer.doit.spark.core

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2
import utils.SparkEnvUtils

import scala.collection.mutable

object MyAccumulatorTest {
  def main(args: Array[String]): Unit = {

    val sc = SparkEnvUtils.getEnv("context", this.getClass.getSimpleName).asInstanceOf[SparkContext]

    val source = sc.parallelize(Seq(
      """{"id": 1, "name": "lisi", "age": 12}""",
      """{"id": "a", "name": "zhangsan", "age": 11}""",
      """{"id": 3, "name": "wangwu"}"""
    ))

    val myAccumulator = new CustomAccumulator(new mutable.HashMap[String, Long])

    sc.register(myAccumulator)

    val res = source.map(json => {
      try {
        val jsonObject = JSON.parseObject(json)
        val id = jsonObject.getInteger("id").toInt
        val name = jsonObject.getString("name")
        val age = jsonObject.getInteger("age").toInt
        (id, name, age)
      } catch {
        case _: NullPointerException => myAccumulator.add(("nullPointerException", 1))
        case _: NumberFormatException => myAccumulator.add(("numberFormatException", 1))
        case _ => myAccumulator.add(("other", 1))
          null
      }
    })

    res.foreach(println)

    println("=============================")

    println(myAccumulator.value)

    sc.stop()
  }
}

class CustomAccumulator(map: mutable.HashMap[String, Long]) extends AccumulatorV2[(String, Long), mutable.HashMap[String, Long]] {
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, Long), mutable.HashMap[String, Long]] = {
    val hashMap = new mutable.HashMap[String, Long]()
    this.map.foreach(kv => hashMap.put(kv._1, kv._2))
    new CustomAccumulator(hashMap)
  }

  override def reset(): Unit = this.map.clear()

  override def add(v: (String, Long)): Unit = {
    val key: String = v._1
    val oldV: Long = this.map.getOrElse(key, 0)
    val newV: Long = v._2
    this.map.put(key, oldV + newV)
  }

  override def merge(other: AccumulatorV2[(String, Long), mutable.HashMap[String, Long]]): Unit = {
    other.value.foreach(kv => {
      val key: String = kv._1
      val oldV: Long = this.map.getOrElse(key, 0)
      val newV: Long = kv._2
      this.map.put(key, oldV + newV)
    })
  }

  override def value: mutable.HashMap[String, Long] = this.map
}