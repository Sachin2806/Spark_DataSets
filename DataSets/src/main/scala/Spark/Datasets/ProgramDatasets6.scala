package Spark.Datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//Aggregating the columns

object ProgramDatasets6 {
  
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
    
    val aggregate = sw_ds.groupByKey(_.species)
                         .agg(max($"height").as[Double], min($"height").as[Double], mean($"weight").as[Double], count($"species").as[Long] )
    aggregate.show()
    
    sw_ds.groupByKey(_.eyecolor).agg(mean($"weight").as[Double]).show()
    //sw_ds.groupByKey(x=>(x.species, x.jedi, x.haircolor)).agg(mean($"weight").as[Double], count($"species").as[Long])
    sw_ds.groupByKey(x=>(x.species, x.jedi, x.haircolor)).agg(mean($"weight").as[Double], count($"species").as[Long] ).show()
    
    //Aggregate Functions - Calculate correlation between columns
    sw_ds.agg(corr($"height", $"weight").as[Double]).show()
    sw_ds.groupByKey(_.jedi).agg(corr($"height", $"weight").as[Double]).show()
    sw_ds.groupByKey(_.species).agg(first($"name").as[String]).show()   
    
    //Sorting
    sw_ds.orderBy($"species".desc, $"weight").show()
    
    //Appending Datasets
    sw_ds.union(sw_ds).show()
  }
  
  case class SW(name: String, gender: String, height: Double, weight: Option[Double], 
                        eyecolor: Option[String], haircolor: Option[String], skincolor: String, 
                        homeland: String, born: String, died: String, jedi: String,
                        species: String, weapon: String)
}