package Spark.Datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//Renaming the columns

object ProgramDatasets4 {
  
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
    
    //1st way of renaming columns by using withColumnRenamed method for fewer columsn
    sw_ds.withColumnRenamed("name", "Name").withColumnRenamed("jedi", "Religion").show()
    
    //2nd way of renaming columns --Where data set is converted to data frame and then 
    //all columns are updated
    
    sw_ds.toDF(Seq("Name", "Gender", "Height", "Weight","Eyecolor","Haircolor","Skincolor",
                   "Homeland","Born","Died","Jedi","Species","Weapon"):_*).as[SW].show()
                   
    sw_ds.toDF(Seq("WHO", "Gender", "Height", "Weight","Eyecolor","Haircolor","Skincolor",
                   "Homeland","Born","Died","Jedi","Species","Weapon"):_*).as[SW1].show()
    
    
  }
  
  case class SW(name: String, gender: String, height: Double, weight: Option[Double], 
                        eyecolor: Option[String], haircolor: Option[String], skincolor: String, 
                        homeland: String, born: String, died: String, jedi: String,
                        species: String, weapon: String)
                        
  case class SW1(WHO: String, gender: String, height: Double, weight: Option[Double], 
                        eyecolor: Option[String], haircolor: Option[String], skincolor: String, 
                        homeland: String, born: String, died: String, jedi: String,
                        species: String, weapon: String)
                        
  case class NameWeight(name: String, weight: Option[Double])
  
}