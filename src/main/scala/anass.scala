//Anass class
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
import org.apache.spark.sql.types
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.DataFrame



class anass(df:DataFrame) {
    var dis= new display(df)
    var sc = new Scanner(System.in)
    val spark=SparkSession
                .builder
                .appName("SparkVSCode")
                .config("spark.master", "local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._

    // Top 10 locations have much people fully vaccinated and people aged older than 65  
    def peoFuVac(): Unit={
        println()
        println("==================== Top 10 locations have much people fully vaccinated and people aged older than 65 ==============")
        println() 
        df.groupBy("location", "aged_65_older").agg((max("people_fully_vaccinated")/max("population")).as("fullyVa_pop")).orderBy(desc("aged_65_older"))show(10)

        //back to the display class
        println("Please Enter the press.........")
        sc.nextLine()
        dis.option()
            
    }
 
    //top 10 country with total_tests/population order by total deaths desc
    def totTestCou(): Unit={
        println()
        println("==================== Top 10 country with max tests and less deaths ================")
        println() 
        val df_new =df.withColumn("total_death_count",$"total_deaths".cast("Int"))
        val df_new2 =df_new.withColumn("total_pop",$"population".cast("Int"))
        val df_new3 =df_new2.withColumn("total_tested",$"total_tests".cast("Int"))
        var df45 = df_new3.select(col("location"),col("total_death_count"),col("total_tested"),col("total_pop")).distinct()
        var dataFrame10 = df45.groupBy("location", "total_death_count", "total_pop").agg(max("total_tested").as("final_total_test")).filter(col("final_total_test").isNotNull).distinct()
        var dataFrame11=dataFrame10
        var dataFrame12=dataFrame11.groupBy("location", "total_death_count").agg((max("final_total_test")/max("total_pop")).as("test_proportion")).filter(col("test_proportion").isNotNull).orderBy(col("total_death_count").desc)
        var dataFrame15=dataFrame12.groupBy(col("location"),col("test_proportion")).agg(max("total_death_count")).show

        //back to the display class
        println("Please Enter the press.........")
        sc.nextLine()
        dis.option()
        
    }

    //SELECT population_density from dataFrame2 where total_deaths/total_population>  order by population density DESC
    def gdpDePo(): Unit={
        println()
        println("==================== Top 5 country with max deaths and max population density ================")
        println() 
        df.groupBy("location","population_density").agg((max("total_deaths")/max("population")).as("TOT_Death_pop")).orderBy(desc("population_density")).show(5)

        //back to the display class
        println("Please Enter the press.........")
        sc.nextLine()
        dis.option()
    }

  
}