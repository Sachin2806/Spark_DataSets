package Spark.Datasets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object ProgramDataset1 {
  
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
    
    val ds1 = Seq(1,2,3,4,5).toDS().show()
    val ds2 = spark.range(5)
    
    ds2.filter(x => x%2 == 0).show()
    ds2.filter(x => x%2 != 0).show()
    spark.range(1).filter('id === 0).explain(true)
  }
}