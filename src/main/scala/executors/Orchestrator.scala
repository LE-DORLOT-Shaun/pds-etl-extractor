package executors

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import workers.DataExtractor.getFileFromURL
import workers.HDFSFileManager.{readCSVFromHDFS, sparkSession}

import scala.util.{Failure, Success}

object Orchestrator {


  def getAndSaveParquet(year : String, month : String): Unit = {


    getFileFromURL(s"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet") match {
      case Success(path: String) => {
        readCSVFromHDFS(path) match {
          case Success(df: DataFrame) => {
            df.show(10)
          }
          case Failure(exception) => {
            println(exception)
          }
        }
      }
      case Failure(exception) => {
        println(exception)
      }
    }
  }
}
