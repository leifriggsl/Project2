
import org.apache.spark.sql.DataFrame
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





class display(df:DataFrame) {
   var scanner = new Scanner(System.in)
    def disp():Unit={

    }

    def option():Unit={
         var a=0
        
            do{
           
                println("Please select the below the option")
                println("(1)See countries with the highest proportion of vaccinated people")
                println("(2)See locations with life expectancy higher than 70 years old")
                println("(3)See the top 10 locations with max tests and less deaths")
                println("(4)See the location with top 10 death rate")
                println("(5)Compare contries with the highest rate of hospital beds per 1000 people to lowest rate of hospital beds per 1000 people (you can choose which year) ")
                println("(6)  Choose a location to query all data for that location. ")
                println("(7)See amount of new cases in the United States between March 2nd, 2021 and March 9th, 2021-Leif")
                println("(8)See the top 10 locations that have the most fully vaccinated people and people aged over 65 ")
                println("(9) See the top 5 locations with max deaths and max population density")
                println("(10)The maximum total cases for each African country in 2021-Leif")
                println("(11) Exit")
                try{
                println("please select the option........")
                a=scanner.nextInt()
                }catch{
                    case e:Exception=>{println("Invalid data Enter(Please enter the Digit)")}
                }
                scanner.nextLine()
                //var s =a.toInt
            if(!((a>=1)&&(a<=11))){
                println()
                println("Please select the number between 1 to 11 ....")
                println()
            }

            }while(!((a>=1)&&(a<=11)))
        
         

//1)See countries with the highest proportion of vaccinated people
//2)See locations with life expectancy higher than 70 years old
//3)See locations with high death rate proportional to population density
//4)See the amount of total Deaths where population is greater than 20,000,000
//5) Compare contries with the highest rate of hospital beds per 1000 people to lowest rate of hospital beds per 1000 people (you can choose which year)-Leif
//6) Choose a location to query all data for that location. 
//7)See amount of new cases in the United States between March 2nd, 2021 and March 9th, 2021-Leif
//8)See the amount of people aged over 65 for countries with the highest proportion of vaccinated people
//9) See countries with the highest proportion of tested people
//10) The maximum total cases for each African country in 2021-Leif





    var har=new hardik(df)
    var ana=new anass(df)
    var le=new leif(df)
    
 a match {
    case 1  => har.PercentPopulationInfected()
    case 2  => har.life_expectancy()
    case 3  => ana.totTestCou()
    case 4  => har.total_deaths()
    case 5  => le.hospitalBedsYear()
    case 6  => har.findWithCountryName()
    case 7  =>le.tenDayUnitedStates()
    case 8  => ana.peoFuVac()
    case 9  =>ana.gdpDePo()
    case 10 => le. maxAfricanCases()
    case 11 =>{
        println()
        println()
        println("Thank You")
    }
    case default => println("Unexpected case: " )
}
    }

 
}


