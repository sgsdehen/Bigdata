package com.lancer.doit.spark.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.SparkEnvUtils

import java.sql.{DriverManager, ResultSet}

object E02_JDBCRDDTest {
  def main(args: Array[String]): Unit = {

    val sc = SparkEnvUtils.getEnv("context", "jdbcRDD").asInstanceOf[SparkContext]

    val getConnection = () => DriverManager.getConnection("jdbc:mysql://localhost:3306/test")

    val sql = "select * from person where age >= ? and age <= ?"

    val mapRow = (rs: ResultSet) => {
      val name = rs.getString(1)
      val age = rs.getInt(2)
      Person(name, age)
    }

    val jdbcRDD: JdbcRDD[Person] = new JdbcRDD[Person](sc, getConnection, sql, 20, 30, 2, mapRow)

    jdbcRDD.foreach(x => println(x))

    sc.stop()
  }
  case class Person(name: String, age: Int) {
    override def toString: String = s"""($name, $age)"""
  }
}
