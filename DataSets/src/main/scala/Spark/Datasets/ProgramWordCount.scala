package Spark.Datasets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions

object ProgramWordCount {
  
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
    
    val data = sc.textFile("C:/Users/CSC/workspace/DataSets/Input/sample.txt")
    data.take(5).foreach(println)
    val dataFlatMap = data.flatMap(x => (x.split(" ")))
    dataFlatMap.take(5).foreach(println)
    val dataMap = dataFlatMap.map(word => (word, 1))
    dataMap.take(5).foreach(println)
    dataMap.reduceByKey(_+_).saveAsTextFile("C:/Users/CSC/workspace/DataSets/Output/wordCount.txt")
    
  }
}