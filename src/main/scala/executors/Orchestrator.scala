package executors

import org.apache.spark.sql.DataFrame
import workers.DataExtractor.getFileFromURL
import workers.DataTransformer.transformDataSilver
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
            // Bronze to Silver
            val hasTransformed = transformDataSilver(df)
            if(!hasTransformed) println("error")
            else println("data transformation success!")

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
