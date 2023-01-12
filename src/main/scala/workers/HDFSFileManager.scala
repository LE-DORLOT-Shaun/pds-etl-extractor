package workers

import helpers.Helper
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * The aim of this class is to encapsulate the logic responsible for reading/writing the data of a given csv file
 */
object HDFSFileManager {
  val sparkHost : String = "local"
  val hdfsRawPath : String = "hdfs://192.168.1.2:9000/raw/locaux"
  val hdfsBronzePath : String = "hdfs://192.168.1.2:9000/bronze/locaux"

  // Spark Session
  val sparkSession: SparkSession = SparkSession
    .builder
    .master(sparkHost)
    .appName("pds-etl-manager")
    .enableHiveSupport()
    .getOrCreate()

  def getAndSaveParquetToHDFS(path: String): Try[DataFrame] = {
    // Log
    println(s"\n${"-" * 25} READING FILE STARTED ${"-" * 25}")
    println(s"reading from ${path}")

    // Spark-session context
    sparkSession.sparkContext.setCheckpointDir("tmp")
    sparkSession.sparkContext.setLogLevel("ERROR")

    // Load data from HDFS
    try{
      val df_raw = sparkSession.read.parquet(path)

      df_raw.show(20)

      var hasWritten : Boolean = writeParquetToHDFS(hdfsRawPath, df_raw)

      if(!hasWritten) Failure(new Throwable("cannot save raw data file"))

      val df_bronze = sparkSession.read
        .schema(Helper.sparkSchemeFromJSON())
        .option("mode", "DROPMALFORMED")
        .parquet(path)

      hasWritten = writeParquetToHDFS(hdfsBronzePath, df_bronze)

      if (!hasWritten) Failure(new Throwable("cannot save bronze data file"))

      Success(df_bronze)
    } catch {
      case _: Throwable =>
        Failure(new Throwable("cannot read file from file"))
    }
  }

  def writeParquetToHDFS(hdfsPath: String, df : DataFrame): Boolean = {
    println(s"${"-" * 25} SAVING FILE STARTED ${"-" * 25}")

    sparkSession.sparkContext.setLogLevel("ERROR")

    try {
      // Write to final
      df.checkpoint(true)
        .write
        .mode(SaveMode.Overwrite)
        .save(hdfsPath)
      true
    }
    catch {
      case _: Throwable =>
        println("error while saving data")
        false
    }
  }
}
