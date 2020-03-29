package Spark.Datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//Adding new columns

object ProgramDatasets5 {
  
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
    
    //Adding a constant column
    sw_ds.withColumn("count", lit(1)).show()
    sw_ds.withColumn("log_weight", log("weight")).show()
//  sw_ds.withColumn("BMI", sw_ds("weight")/(sw_ds("height")*sw_ds("height")/10000)).show()
    sw_ds.withColumn("hashed_hair", hash(sw_ds("haircolor"))).show()
    
    //Filtering the rows
    sw_ds.filter(x => x.name == "Anakin Skywalker").show()
    sw_ds.filter(x => x.eyecolor== "brown").show()
    sw_ds.filter(x => x.eyecolor== Some("brown")).show()
    sw_ds.filter(x => x.eyecolor.getOrElse("") == "brown").show()
    sw_ds.filter(x =>  x.height < 100).show()
    
    sw_ds.filter(x => x.weight match {case Some(y) => y>=79 case None => false} ).show()
  }
  
  case class SW(name: String, gender: String, height: Double, weight: Option[Double], 
                        eyecolor: Option[String], haircolor: Option[String], skincolor: String, 
                        homeland: String, born: String, died: String, jedi: String,
                        species: String, weapon: String)
}