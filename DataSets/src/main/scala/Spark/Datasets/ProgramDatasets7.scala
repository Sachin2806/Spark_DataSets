package Spark.Datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//Other useful functions in Dataset

object ProgramDatasets7 {
  
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
    
    val friends_ds = spark.read
                          .format("com.databricks.spark.csv")
                          .option("header", "true")
                          .option("inferSchema", "true")
                          .load("C:/Users/CSC/workspace/DataSets/Input/starWars1.csv")                          
    
    val characters_ds = spark.read
                             .format("com.databricks.spark.csv")
                             .option("header", "true")
                             .option("inferSchema", "true")
                             .load("C:/Users/CSC/workspace/DataSets/Input/starWars1.csv")                             
    
    val bad_join_df = characters_ds.join(friends_ds, characters_ds.col("name") === friends_ds.col("name"))
    bad_join_df.show()
   
    val sw_df = characters_ds.join(friends_ds, Seq("name", "gender", "height",
                                                    "weight" , "eyecolor" , "haircolor", 
                                                    "skincolor" , "homeland" , "born", 
                                                    "died" , "jedi" , "species" , "weapon"))
    sw_df.show()
    
    val sw_ds = sw_df.as[SW]
    sw_ds.show()
    
    //get the number of records by using count()
    println(sw_ds.count())
    
    //get the number of columns by using .columns.size
    println(sw_ds.columns.size)
    
    //get the schema by using printSchema or by dtypes
    println(sw_ds.printSchema())
    sw_ds.dtypes
    
    //Create a column containing random numbers.
    sw_ds.withColumn("random",rand).show()
    
    //Calculate the lenght of strings in a column
    sw_ds.withColumn("column_length", length(sw_ds("name"))).show()
    
    //get the levenshtein distance between two string columns
    sw_ds.withColumn("name_species_diff", levenshtein(sw_ds("name"), sw_ds("species"))).show()
    
    //Find the location of a substring within a string by using locate
    sw_ds.withColumn("locate_string", locate("L", sw_ds("name"))).show()
    
  }
  
  case class SW(name: String, gender: String, height: Double, weight: Option[Double], 
                        eyecolor: Option[String], haircolor: Option[String], skincolor: String, 
                        homeland: String, born: String, died: String, jedi: String,
                        species: String, weapon: String)
}