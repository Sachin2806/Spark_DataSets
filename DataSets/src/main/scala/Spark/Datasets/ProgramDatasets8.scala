package Spark.Datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//Other useful functions in Dataset

object ProgramDatasets8 {
  
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
    
    //To convert a sequence to a Dataset, call .toDS() on the sequence.
    val dataset = Seq(1, 2, 3, 4, 5).toDS()
    dataset.show()
    
    //Sequence of case classes, calling .toDS()
    val personDS = Seq(("Max", 33), ("Adam", 32), ("Muller", 62))
    personDS.toDF("name", "age").as[Person].show()
    
    //Create a Dataset from an RDD
    val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))
    val integerDS = rdd.toDS()
    integerDS.show()
    
    //Create a Dataset from a DataFrame
    val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
    val df = sc.parallelize(inputSeq).toDF("name", "foundingYear", "numEmployees")
    val companyDS = df.as[Company]
    companyDS.show()
    
    //Deal with tuples while converting a DataFrame to Dataset without using a case class.
    val rdd1 = sc.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
    val df1 = rdd1.toDF("id", "name")
    val dataset1 = df1.as[(Int, String)]
    dataset1.show()
    
    //word count example using data set
    val wordDataSet =  sc.parallelize(Seq("Spark I am your father", "May the spark be with you", " Spark I am your father")).toDS()
    val groupedDataset = wordDataSet.flatMap(x => x.toLowerCase.split(" "))  // Convert to lowercase and then split on whitespace
                                    .filter(_ != "")                         // Filter empty words
                                    .groupBy("value")
    val countsDataset = groupedDataset.count().orderBy("count")
    countsDataset.show()
    
    //Convert a Dataset to a DataFrame
    val result = wordDataSet.flatMap(_.split(" "))        // Split on whitespace
                            .filter(_ != "")              // Filter empty words
                            .map(_.toLowerCase())
                            .toDF()                       // Convert to DataFrame to perform aggregation/sorting
                            .groupBy("value")             // Count number of occurrences of each word
                            .agg(count("*") as "numOccurances")
                            .orderBy($"numOccurances" desc)  // Show most common words first
                            
    result.show()
  }
  
  case class Person(name: String, age: Int)
  case class Company(name: String, foundingYear: Int, numEmployees: Int)
}