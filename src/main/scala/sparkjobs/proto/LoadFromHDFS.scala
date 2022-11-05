package sparkjobs.proto

import org.apache.spark.sql.SparkSession

object LoadFromHDFS {

  val hdfsHost: String = "hdfs://192.168.1.2:9000/bronze/titanic.csv"
  val sparkHost: String = "local"

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

    println(s"FINISH ${"!"*50}")
    println(df)


  }

}
