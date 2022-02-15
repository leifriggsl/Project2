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



class QuerL3(df:DataFrame, spark:SparkSession) {
 
    def peoFuVac(): Unit={
        println("==================== Top 10 locations have much people fully vaccinated and people aged older than 65 ==============")
  

    //val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
        df.groupBy("location", "aged_65_older").agg((max("people_fully_vaccinated")/max("population")).as("fullyVa_pop")).orderBy(desc("aged_65_older"))show(10)


    
}
 
  //top 10 country with total_tests/population order by country desc
    def totTestCou(): Unit={
        println("==================== Top 10 country with max tests and less deaths ================")
       

        //val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
        //df.groupBy("location","total_deaths").agg(max("total_tests")/max("population")).orderBy(desc("total_deaths")).show(10)
        df.createOrReplaceTempView("new_case")
        spark.sql("SELECT location, total_deaths, max(total_tests)/max(population) test_per_pop FROM new_case GROUP BY location, total_deaths ORDER BY total_deaths DESC LIMIT 10").show(10)
        //spark.sql("SELECT location, total_deaths, max(total_tests)/max(population) test_per_pop FROM new_case WHERE total_deaths!='Null' AND test_per_pop!='Null' GROUP BY location, total_deaths ORDER BY total_deaths ASC LIMIT 10").show(10)
        
    }
    //SELECT population_density from dataFrame2 where total_deaths/total_population>  order by population density DESC
    def gdpDePo(): Unit={
        println("==================== Top 5 country with max deaths and max population density ================")
        //df.groupBy("location").agg(max("total_deaths") / max("total_population")).orderBy(desc("gdp_per_capita")).show(20)
        df.groupBy("location","population_density").agg((max("total_deaths")/max("population")).as("TOT_Death_pop")).orderBy(desc("population_density")).show(5)
        //df2.withColumnRenamed("max(total_deaths)/max(population)", ).show(5)

    }
    // Top 10 country in same conitent has handlde cases of covid19 
    def toCaTdeaths(): Unit={
        var scanner =new Scanner(System.in)
        println("Chose the continent which is handlde better the cases of covid19 :")
        var conCase = scanner.nextLine()
        df.createOrReplaceTempView("new_case")
        spark.sql("SELECT continent, location, (MAX(total_cases) - MAX(total_deaths)) efficent_handling FROM new_case WHERE continent="+ conCase + "GROUP BY location ORDER BY efficent_handling DESC LIMIT 10")
        //spark.sql("SELECT continent, location, (MAX(total_cases) - MAX(total_deaths)) efficent_handling FROM new_case WHERE continent=Asia GROUP BY location ORDER BY efficent_handling DESC LIMIT 10")
    }

  
}

