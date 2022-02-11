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
import org.apache.spark.sql.functions.desc
//import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types


//imported all the libraries
object Project2 {
    def main(args: Array[String]): Unit = {
        var adminChoice=""
        var scanner =new Scanner(System.in)
        // This block of code is all necessary for spark/hive/hadoop
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // change if winutils.exe is in a different bin folder
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1Ex")    // Change to whatever app name you want
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._


//countries where total_vaccinated/total_population order by desc
//***countries with life expectancy > 70
//SELECT gdp_per_capita from dataFrame2 where total_deaths/total_population>  order by gdp_per_capita  DESC
//total deaths where population >20,000,000 desc
//top 10 countries who have hospital_beds_per_thousand in 2020
//input variable for location, then query all records for that location.  Otherwise, "This is not a given location"
//enter duration, see graph for new cases during that duration
//top 10 aged_65_older where people_fully_vaccinated/population
//top 10 country with total_tests/population order by country desc



val spark=SparkSession
.builder
.appName("SparkHelloWorld")
.config("spark.master", "local")
.config("spark.eventLog.enabled", "false")
.getOrCreate()
//println("Hello Spark!")
val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
//val dataFrame2=dataFrame.toDF("iso_code","continent","location","date","total_cases","new_cases","total_deaths","new_deaths","new_tests","total_tests","total_vaccinations","people_vaccinated","people_fully_vaccinated","population","population_density","median_age","aged_65_older","aged_70_older","gdp_per_capita","hospital_beds_per_thousand","life_expectancy")
//dataFrame.groupBy("location").agg(max("total_vaccinations")/max("population")).show(10)
//dataFrame.groupBy("location").agg(max("hospital_beds_per_thousand")).show(10)
//dataFrame.groupby().max().show(20)
//dataFrame.groupBy("aged_65_older").agg(max("people_fully_vaccinated")/max("population")).show(10)
//dataFrame.groupBy("location", "total_deaths").agg(max("total_tests")/max("population")).show(10)
var qureClass = new QuerL3(dataFrame)
qureClass.totTestCou()
qureClass.peoFuVac()
qureClass.gdpDePo()

//dataFrame.show()
//MaxTotalDeaths(sparkCtx:SparkContext)
//dataFrame.select("location").show(10)
//dataFrame.select("location","life_expectancy").filter(dataFrame("life_expectancy") > 70).distinct().show(20)

//dataFrame.select(filter("life_expectancy" \'>'\ 70)).show(20)
spark.stop()
//dataFrame.where()

}

/*def MaxTotalDeaths(sparkCtx:SparkContext): Unit = {
        val result = ("SELECT DISTINCT(continent), total_deaths, FROM dataFrame2 WHERE total_deaths >20000000 ORDER BY total_deaths DESC")
      println(result)
      //  result.show()
*/   // }//


} 
