package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions.{avg, broadcast, col, max}
//import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types

class QuerL3 {
    peoFuVac()
    totTestCou()
    def peoFuVac(): Unit={
    val spark=SparkSession
    .builder
    .appName("SparkHelloWorld")
    .config("spark.master", "local")
    .config("spark.eventLog.enabled", "false")
    .getOrCreate()

    val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
    dataFrame.groupBy("aged_65_older").agg(max("people_fully_vaccinated")/max("population")).show(10)

    spark.stop()
}
    //top 10 country with total_tests/population order by country desc
    def totTestCou(): Unit={
        val spark=SparkSession
        .builder
        .appName("SparkHelloWorld")
        .config("spark.master", "local")
        .config("spark.eventLog.enabled", "false")
        .getOrCreate()

        val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
        dataFrame.groupBy("location", "total_deaths").agg(max("total_tests")/max("population")).show(10)

        spark.stop()
    }
  
}

