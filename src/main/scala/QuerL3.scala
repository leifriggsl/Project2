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
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.DataFrame



class QuerL3(df:DataFrame) {
 
    def peoFuVac(): Unit={
        println("==================== Top 10 locations have much people fully vaccinated and people aged older than 65 ==============")
  

    //val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
        df.groupBy("location", "aged_65_older").agg(max("people_fully_vaccinated")/max("population")).show(10)

    
}
 
  //top 10 country with total_tests/population order by country desc
    def totTestCou(): Unit={
        println("==================== Top 10 country with max tests and less deaths ================")
       

        //val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
        df.groupBy("location","total_deaths").agg(max("total_tests")/max("population")).orderBy(desc("total_deaths")).show(10)

        
    }
    //SELECT gdp_per_capita from dataFrame2 where total_deaths/total_population>  order by gdp_per_capita  DESC
    def gdpDePo(): Unit={
        println("==================== Top 5 country with max deaths and max population density ================")
        //df.groupBy("location").agg(max("total_deaths") / max("total_population")).orderBy(desc("gdp_per_capita")).show(20)
        df.groupBy("location","population_density").agg(max("total_deaths")/max("population")).orderBy(desc("population_density")).show(5)
    }
  
}

