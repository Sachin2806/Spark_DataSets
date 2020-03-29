package Spark.Datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProgramJoinsDatasets1 {
  
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
    
    //If we use map, then the result is a Dataset so the column types are inherited but the 
    //column names are lost.
    val sw_dsMap = sw_ds.map(x => (x.name, x.weight))
    sw_dsMap.show()
    
    //If we use select and the column names, then the result is a DataFrame, so the type of the 
    //columns are lost.
    val sw_dsDF = sw_ds.select("name", "weight")
    sw_dsDF.show()
    
    //If we use select and provide the column names AND the column types, then the result is a Dataset with 
    //seemingly proper column names and proper types.
    val sw_dsDS = sw_ds.select($"name".as[String], $"weight".as[Double])
    sw_dsDS.show()
    
    //Corrected Map
    val sw_dsMap_Cor = sw_ds.map(x => NameWeight(x.name, x.weight))
    sw_dsMap_Cor.show()
    
    //Corrected Dataframe
    val sw_dsDF_Cor = sw_ds.select("name", "weight").as[NameWeight]
    sw_dsDF_Cor.show()
    
    val sw_dsDS_Cor = sw_ds.select($"name".as[String], $"weight".as[Double]).as[NameWeight]
    sw_dsDS_Cor.show()
    
  }
  
  case class SW(name: String, gender: String, height: Double, weight: Option[Double], 
                        eyecolor: Option[String], haircolor: Option[String], skincolor: String, 
                        homeland: String, born: String, died: String, jedi: String,
                        species: String, weapon: String)
                        
  case class NameWeight(name: String, weight: Option[Double])
  
}