package Spark.Datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._


object ProgramDataset3 {
  
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
    
    val starWars_ds = spark
                     .read
                     .format("com.databricks.spark.csv")
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .load("C:/Users/CSC/workspace/DataSets/Input/starWars1.csv")
                     .as[Characters]
    starWars_ds.write
               .format("com.databricks.spark.csv")
               .mode("overwrite")
               .option("mode", "OVERWRITE")
               .save("C:/Users/CSC/workspace/DataSets/Output/starWars")
               
    val characters_BadType_ds = spark.read
                                     .format("com.databricks.spark.csv")
                                     .option("header", "true")
                                     .option("inferSchema", "true")
                                     .load("C:/Users/CSC/workspace/DataSets/Input/starWars1.csv")
                                     .as[Characters_BadType]
    
//    val characters_BadType_filter1 = characters_BadType_ds.filter(x => x.jedi == "no_jedi")
//    characters_BadType_filter1.show()
//    
//    val characters_BadType_filter2 = characters_BadType_ds.filter(y => y.haircolor == "brown")
//    characters_BadType_filter2.show()
    
      val characters_BadType_ds2 = characters_BadType_ds.filter(x=> x.jedi=="no_jedi")
      characters_BadType_ds2.show()
      characters_BadType_ds2.filter(x=> x.skincolor=="light").show()
    
  }
  
  case class Characters(name: String, gender: String, height: Double, weight: Option[Double], 
                        eyecolor: Option[String], haircolor: Option[String], skincolor: String, 
                        homeland: String, born: String, died: String, jedi: String,
                        species: String, weapon: String)
                        
  case class Characters_BadType(name: String, gender: String, height: Double, weight: Option[Double], 
                        eyecolor: Option[String], haircolor: Option[String], skincolor: String, 
                        homeland: String, born: String, died: String, jedi: String,
                        species: String, weapon: String)  
  
}