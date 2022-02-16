//This class is a DIsplay class
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






class display(df:DataFrame) {
   var scanner = new Scanner(System.in) // calling a Scanner method
    def disp():Unit={}

    def option():Unit={
         var a=0
        
            do{
           //Display this information
                println()
                println()
                println("--------------------Covid 19 Data Exploration-------------------- ")
                println("Please select the below the option")
                println()
                println("(1)See countries with the highest proportion of vaccinated people")
                println()
                println("(2)See locations with life expectancy higher than 75 years old")
                println()
                println("(3)See the top 10 locations with max tests and less deaths")
                println()
                println("(4)See the location with top 10 death rate")
                println()
                println("(5)Compare contries with the highest rate of hospital beds per 1000 people to lowest rate of hospital beds per 1000 people (you can choose which year) ")
                println()
                println("(6)Choose a location to query all data for that location. ")
                println()
                println("(7)See amount of new cases in the United States between March 2nd, 2021 and March 12th, 2021")
                println()
                println("(8)See the top 10 locations that have the most fully vaccinated people and people aged over 65 ")
                println()
                println("(9) See the top 5 locations with max deaths and max population density")
                println()
                println("(10)The maximum total cases for each African country in 2021")
                println()
                println("(11) Exit")
                println()
                println("--------------------------------------------------------------------------------")
                println()
                try{
                println("please select the option........")
                a=scanner.nextInt()
                }catch{
                    case e:Exception=>{println("Invalid data Enter(Please enter the Digit)")}
                }
                scanner.nextLine()
                
            if(!((a>=1)&&(a<=11))){
                println()
                println("Please select the number between 1 to 11 ....")
                println()
            }

            }while(!((a>=1)&&(a<=11)))
        
        





        var har=new hardik(df)  //calling hardik class
        var ana=new anass(df)   //calling anass class
        var le=new leif(df) //classing leif class


        // calling particular method in different clas(dependent user input)    
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
                println("Thank You!")
                }
            case default => println("Unexpected case: " )
            }
        }
    }


