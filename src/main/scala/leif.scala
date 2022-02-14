import org.apache.spark.sql.DataFrame
//package org.apache.spark.sql.execution
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
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
import org.apache.spark.sql.functions.{col, to_date}

class leif(df:DataFrame){
 val spark=SparkSession
 .builder
 .appName("SparkVSCode")
 .config("spark.master", "local")
 .getOrCreate()
 spark.sparkContext.setLogLevel("WARN")
 import spark.implicits._
 var chooseYear=""


 //1)See countries with the highest proportion of vaccinated people
//***2)See locations with life expectancy higher than 70 years old
//3)See locations with high death rate proportional to population density
//4)See the amount of total Deaths where population is greater than 20,000,000
//5) Compare contries with the highest rate of hospital beds per 1000 people to lowest rate of hospital beds per 1000 people (you can choose which year)
//6) Choose a location to query all data for that location. 
//7)See amount of new cases in the United States between March 2nd, 2021 and March 9th, 2021
//8)See the amount of people aged over 65 for countries with the highest proportion of vaccinated people
//9) See countries with the highest proportion of tested people
//10) The maximum total cases for each African country in 2021


 tenDayUnitedStates()
 hospitalBedsYear()
 maxAfricanCases()
       








  def tenDayUnitedStates(): Unit = {
    val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
    var dataFrame3=dataFrame

    val modDate=udf((date:String) => {
          val dateArr = date.split("/")
          var day =dateArr(1)
          var month=dateArr(0)
         var year = dateArr(2)
        

          if (day.length==1)
              {day="0" +day}

          if (month.length==1)
             {month= "0" + month}

          year = year.substring(2,4)

          day + "-" + month + "-" + year
      })
    var dataFrame4=dataFrame3.withColumn("date_edited", modDate($"date"))
    dataFrame4.select(col("date_edited"), to_date(col("date_edited"), "MM-dd-yy").as("date_edited"))
    dataFrame4.createOrReplaceTempView("new_Dates")
    println("The new cases rate in the United States between 03-01-20 and 03-09-20 is as follows.")  
    println("")
    spark.sql("SELECT location, Max(new_cases), date_edited FROM new_Dates WHERE date_edited >= '03-01-20' AND date_edited <= '03-09-20' AND location='United States' AND date_edited LIKE '%21' GROUP BY date_edited, location ").show()
    //spark.sql("SELECT * FROM MarchDates WHERE date_edited LIKE '%20' ").show
 }


 def hospitalBedsYear(): Unit = {
        var chooseYear=""
        val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
        var dataFrame3=dataFrame

        val modDate=udf((date:String) => {
          val dateArr = date.split("/")
          var day =dateArr(1)
          var month=dateArr(0)
          var year = dateArr(2)
        

          if (day.length==1)
              {day="0" +day}

          if (month.length==1)
             {month= "0" + month}

          year = year.substring(2,4)

          day + "-" + month + "-" + year
      })
    var dataFrame4=dataFrame3.withColumn("date_edited", modDate($"date"))
    dataFrame4.select(col("date_edited"), to_date(col("date_edited"), "MM-dd-yy").as("date_edited"))
    dataFrame4.createOrReplaceTempView("new_Dates")
    println("Please select a year 20 for 2020, 21 for 2021, or 22 for 2020 to see which countries had the most hospital beds, and also which countries had the least hospital beds.  Please enter 'q' to quit")
    var scanner =new Scanner(System.in)
    chooseYear=scanner.nextLine()
    var chooseYear2=chooseYear
    while(chooseYear!="q")
   { 
         if(chooseYear2=="20"||chooseYear2=="21"||chooseYear2=="22")
            {println("The countries with the most hospital beds per 1000 people for the year are as follows.")
            println("")
            spark.sql("SELECT location, hospital_beds_per_thousand FROM new_Dates WHERE date_edited Like '%"+chooseYear2+"' GROUP BY location, hospital_beds_per_thousand  ORDER BY hospital_beds_per_thousand DESC  ").show()
            println("The countries with the least hospital beds per 1000 people for the year are as follows.")
            println("")
            spark.sql("SELECT location, hospital_beds_per_thousand FROM new_Dates WHERE date_edited Like '%"+chooseYear2+"' AND hospital_beds_per_thousand!='Null'  GROUP BY location, hospital_beds_per_thousand ORDER BY hospital_beds_per_thousand ASC ").show()
        }
        else{println("This is not a valid year.")}
        println("Please select a year 20 for 2020, 21 for 20201, or 22 for 2020 to see which countries had the most hospital beds, and also which countries had the least hospital beds.  Please enter 'q' to quit")
        chooseYear=scanner.nextLine()
        chooseYear2=chooseYear
    }
 }
    def maxAfricanCases(): Unit = {
        import spark.implicits._
        val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")
        var dataFrame3=dataFrame
        println("The maximum reported total cases for each African country in 2021 is as follows.")
        println("")
        var dataFrame4 = dataFrame3.select(col("location"),col("continent"),col("total_cases"),col("date")).filter((col("continent") === "Africa") && (col("total_cases").isNotNull) && (col("date").like("%21"))).orderBy(col("total_cases").desc).distinct
        var dataFrame5= dataFrame4.groupBy(col("location")).agg(max(col("total_cases")).as("max_cases"))
       dataFrame5.show
     
     
      }



}