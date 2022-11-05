package sparkjobs.proto

import org.apache.spark.sql.SparkSession

object LoadFromHDFS {

  val hdfsHost: String = "HDFS://172.31.252.100:9000/bronze/titanic.csv"
  val sparkHost: String = "spark://172.31.252.100:7077"

  def loadFromHDFS: Unit = {

    val sparkSession = SparkSession
      .builder
      .master(sparkHost)
      .appName("Spark CSV Reader")
      .getOrCreate()

    val df = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(hdfsHost)

    println(df)


  }

}
