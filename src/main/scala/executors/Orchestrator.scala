package executors

import org.apache.spark.sql.DataFrame
import workers.DataExtractor.getFileFromURL
import workers.HDFSFileManager.readParquetFromFile

import scala.util.{Failure, Success}

object Orchestrator {


  def getAndSaveParquet(year : String, month : String): Unit = {

//    getFileFromURL(s"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet") match {
//      case Success(path: String) => {
//        println("file downloaded successfully")
//        readParquetFromFile(s"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet") match {
//          case Success(df: DataFrame) => {
//            df.show(10)
//          }
//          case Failure(exception) => {
//            println(exception)
//          }
//        }
//      }
//      case Failure(exception) => {
//        println(exception)
//      }
//    }
    readParquetFromFile(s"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet") match {
      case Success(df: DataFrame) => {
        df.show(10)
      }
      case Failure(exception) => {
        println(exception)
      }
    }
  }
}
