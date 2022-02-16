//main barch
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

        // This block of code is all necessary for sparkSession
        
        val spark=SparkSession
        .builder
        .appName("SparkHelloWorld")
        .config("spark.master", "local")
        .config("spark.eventLog.enabled", "false")
        .getOrCreate()

        //reading a csv file 
        val dataFrame=spark.read.option("header", "true").csv("src/main/resources/covid-data.csv")

        //go to display class
        var di=new display(dataFrame)
        // calling a method in display class
        di.option()

    }
}