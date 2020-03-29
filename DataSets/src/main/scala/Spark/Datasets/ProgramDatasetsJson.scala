package Spark.Datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset

//Other useful functions in Dataset

object ProgramDatasetsJson {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                  .setAppName("ProgramJson2")
                  .setMaster("local")
    val spark = SparkSession
               .builder()
               .appName("ProgramJson2")
               .config(conf)
               .config("spark.master", "local")
               .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
               .getOrCreate()
               
    val sc = spark.sparkContext
    import spark.implicits._
    
    //val ds = spark.read.json()
    //val df = ds.toDF()
    
    val data = spark.read.json("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/employee.json")
    val ds = data.as[Employee]
    ds.show()
  }
  
  case class Employee(age:String, id:String, name:String)
}