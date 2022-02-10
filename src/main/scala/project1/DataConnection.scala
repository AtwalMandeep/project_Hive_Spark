package project1
import org.apache.spark.sql.SparkSession

class DataConnection {

  def dataCon(): SparkSession = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession.builder()
      .appName("problemAssign")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
       println("created spark session")
      spark.sparkContext.setLogLevel("WARN")
    return spark
  }
}