package workers

import java.net.URL
import java.io.File
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object DataExtractor {

  def getFileFromURL(url : String): Try[String] = {

    try {
      val arr = url.split('/')
      val temp_path = s"/tmp/${
        arr {
          arr.length - 1
        }
      }"

      fileDownloader(url, temp_path)
      Success(temp_path)

    } catch {
      case e: java.io.IOException => Failure(new Throwable(s"error occurred: ${e.getMessage}"))
    }
  }

  def fileDownloader(url: String, filename: String) = {
    import sys.process._
    new URL(url) #> new File(filename) !!
  }
}


