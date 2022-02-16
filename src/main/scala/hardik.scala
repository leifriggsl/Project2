
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
import org.apache.spark.sql.types
import org.apache.spark.sql._
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column




class hardik(df:DataFrame) {
    var dis= new display(df)
    var sc = new Scanner(System.in)

    //Start SparkSession
    val spark=SparkSession
                    .builder
                    .appName("SparkVSCode")
                    .config("spark.master", "local")
                    .getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            import spark.implicits._


    //)See locations with life expectancy higher than 75 years old
    def life_expectancy():Unit = {
        println()
        println("==================== See locations with life expectancy higher than 75 years ================")    
        var df_1 =df.where(df("life_expectancy")> 70).groupBy("location","life_expectancy").agg(max("population")).orderBy(desc("life_expectancy")).select("location","life_expectancy").distinct

        df_1.show(100,50)
    
        //back to the display class
        println("Please Enter the press.........")
        sc.nextLine()
        dis.option()

    } 



    //See countries with the highest proportion of vaccinated people-
    def PercentPopulationInfected():Unit={
        println()
        println("==================== See countries with the highest percentage of vaccinated people ================")
        println() 
        var  df_new=df.withColumn("people_fully_vaccinateds",$"people_fully_vaccinated".cast("Int")).withColumn("populations",$"population".cast("Int"))
        df_new.createOrReplaceTempView("df1")
    
        var query = "Select location, population, MAX(total_cases) as total_cases,  round(Max(people_fully_vaccinateds/populations)*100,2)  PercentPopulationvaccinated "+ "  From df1 Group by Location, Population order by PercentPopulationvaccinated desc"

        val f= spark.sql(query)
        f.show(20)
        //f.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").csv("output/percentPopulation")

         //back to the display class
        println("Please Enter the press.........")
        sc.nextLine()
        dis.option()
    }
    

    //See the locations with top 10 death rate
    def total_deaths():Unit={
        println()
        println("====================See the locations with top 10 death rate ================")
        println()   
        var  df_new=df.withColumn("total_death_count",$"total_deaths".cast("Int")).withColumn("new_cases_count",$"total_cases".cast("Int"))
        val df_new2= df_new.groupBy("location").agg(max("total_death_count"),max("new_cases_count")).orderBy(desc("max(total_death_count)"))
        val df_new3= df_new2.filter(df_new2("max(total_death_count)")< 969519).limit(10)
        df_new3.show(10)
        
        //df_new3.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").csv("output/totaldeath")

        //back to the display class
        println("Please Enter the press.........")
        sc.nextLine()
        dis.option()

    }



    //Choose a location to query all data for that location. 
    def findWithCountryName():Unit={
        println()
        println("====================Choose a location to query all data for that location ================")
        println()
        try{
        var coun=" "
        var a=0
        while(a==0){
            println("Enter the Country name:")
            coun=sc.nextLine().trim().toLowerCase()
            //sc.nextLine()

            //check the user enter correct data
            validate(coun)
            var name=coun.capitalize
            
            var f = df.select("*").filter(col("location") === name)
            
            if(f.count==0){
                a=0
                println("**************************")
                println("please enter the correct name !!!!")
                println("**************************")
                println()
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
    
    //check the bad data
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

    
}