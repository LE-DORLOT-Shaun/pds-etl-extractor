package executors

import org.apache.spark.sql.DataFrame
import workers.DataExtractor.getFileFromURL
import workers.HDFSFileManager.getAndSaveParquetToHDFS

import java.io.File
import scala.util.{Failure, Success}

object Orchestrator {


  def getAndSaveParquet(year : String, month : String): Unit = {

    getFileFromURL(s"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet") match {
      case Success(path: String) => {
        println("file downloaded successfully")
        getAndSaveParquetToHDFS(path) match {
          case Success(df: DataFrame) => {
            df.show(20)

            // Delete Temp File
            new File(path).delete()
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
