
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
import org.apache.spark.sql.functions.{min, max,desc,sum}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.expressions.scalalang.typed
 import org.apache.spark.sql.functions.{avg, broadcast, col, max}
//import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types
import org.apache.spark.sql._
import org.apache.spark.sql.RelationalGroupedDataset

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column




class hardik(df:DataFrame) {
var dis= new display(df)
var sc = new Scanner(System.in)
//life expectancy > then 70 
 val spark=SparkSession
                .builder
                .appName("SparkVSCode")
                .config("spark.master", "local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._



def life_expectancy():Unit = {
        
var df_1 =df.where(df("life_expectancy")> 70).groupBy("location","life_expectancy").agg(max("population")).orderBy(desc("life_expectancy")).select("location","life_expectancy").distinct
df_1.show(100,50)
println()
//df_1.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").csv("output/lifeepectancy")
println("Please Enter the press.........")
sc.nextLine()
dis.option()

} 






//countries where total_vaccinated/total_population order by desc//hardik
def PercentPopulationInfected():Unit={
    var  df_new=df.withColumn("people_fully_vaccinateds",$"people_fully_vaccinated".cast("Int")).withColumn("populations",$"population".cast("Int"))
    df_new.createOrReplaceTempView("df1")
   
var query = "Select location, population, MAX(total_cases) as total_cases,  Max((people_fully_vaccinateds/populations))*100 as PercentPopulationInfected "+ "  From df1 Group by Location, Population order by PercentPopulationInfected desc"



   val f= spark.sql(query)
   f.show(20)
    f.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").csv("output/percentPopulation")
    println("Please Enter the press.........")
sc.nextLine()
dis.option()
}
  


//total deaths where population >20,000,000 desc
def findWithCountryName():Unit={
    var sc = new Scanner(System.in)
    try{
    var coun=" "
    var a=0
    while(a==0){
        println("Enter the Country name:")
       coun=sc.nextLine().trim().toLowerCase()
        //sc.nextLine()
        validate(coun)
        var name=coun.capitalize
        ///println(coun)
        var f = df.select("*").filter(col("location") === name)
        
        if(f.count==0){
            a=0
            println("please enter the correct name !!!!")
        }
        else{a=1
            var na =df.select("*").filter(col("location") === name)
            na.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").csv("output/namelocation")
            na.show(10000)
            println("Please Enter the press.........")
            sc.nextLine()
            dis.option()
        }
    }
    }catch{
        case e:Exception=>{println("Invalid data can you try again")
    findWithCountryName()
    }
    }
}

 def validate(s:String):Unit=
    {
        if(s.length==0){
                throw new MyException("Invalid data Enter(Please enter the Alphabet) ")
            }
        if(s.isEmpty){
                throw new MyException("Invalid data Enter(Please enter the Alphabet) ")
            }
        var asd =s.toUpperCase()
        for(i <- 0 to asd.length-1 by 1)
        {
            if(!(asd(i)>='A'&&asd(i)<='Z')){
            throw new MyException("Invalid data Enter(Please enter the Alphabet) ")
            }
        }
    }

//top 10 country with total_tests/population order by country desc
    def total_deaths():Unit={
        //     val df_new =df.withColumn("total_death_count",$"total_deaths".cast("Int"))
//     val df_new2=df_new.groupBy("location").max("total_death_count").orderBy(desc("max(total_death_count)")).limit(20)
//    val df_new3= df_new2.filter(df_new2("max(total_death_count)")< 969519)
//    df_new3.show(10)
var  df_new=df.withColumn("total_death_count",$"total_deaths".cast("Int")).withColumn("new_cases_count",$"total_cases".cast("Int"))

    
   val df_new2= df_new.groupBy("location").agg(max("total_death_count"),max("new_cases_count")).orderBy(desc("max(total_death_count)"))
    val df_new3= df_new2.filter(df_new2("max(total_death_count)")< 969519).limit(10)
    df_new3.show(10)
    df_new3.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").csv("output/totaldeath")
println("Please Enter the press.........")
sc.nextLine()
dis.option()

}

}