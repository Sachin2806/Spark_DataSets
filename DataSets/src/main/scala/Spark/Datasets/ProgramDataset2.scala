package Spark.Datasets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object ProgramDataset2 {
  
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
    
    val df = Seq(("Yoda", "Obi-Wan Kenobi"),
                 ("Anakin Skywalker", "Sheev Palpatine"),
                 ("Luke Skywalker",   "Han Solo, Leia Skywalker"),
                 ("Leia Skywalker",   "Obi-Wan Kenobi"),
                 ("Sheev Palpatine",  "Anakin Skywalker"),
                 ("Han Solo",         "Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi, Chewbacca"),
                 ("Obi-Wan Kenobi",   "Yoda, Qui-Gon Jinn"),
                 ("R2-D2",            "C-3PO"),
                 ("C-3PO",            "R2-D2"),
                 ("Darth Maul",       "Sheev Palpatine"),
                 ("Chewbacca",        "Han Solo"),
                 ("Lando Calrissian", "Han Solo"),
                 ("Jabba",            "Boba Fett")).toDF("name", "friends")
    df.show()
    //.as[] converts DataFrame "df" as a Dataset
    val friends_DS = df.as[Friends]
    friends_DS.show()
    
    val ds_missing = Seq(("Yoda",             Some("Obi-Wan Kenobi")),
                      ("Anakin Skywalker", Some("Sheev Palpatine")),
                      ("Luke Skywalker",   None),
                      ("Leia Skywalker",   Some("Obi-Wan Kenobi")),
                      ("Sheev Palpatine",  Some("Anakin Skywalker")),
                      ("Han Solo",            Some("Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi")))
                     .toDF("Who", "friends")
                     .as[Friends_Missing]
                     .show()
  
     
     
  }
  case class Friends(name: String, friends: String)
  case class Friends_Missing(Who: String, friends: Option[String])
}