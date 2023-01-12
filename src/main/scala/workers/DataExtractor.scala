package workers

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

      val src = scala.io.Source.fromURL(url)
      val out = new java.io.FileWriter(temp_path)
      out.write(src.mkString)
      out.close()
      Success(temp_path)

    } catch {
      case e: java.io.IOException => Failure(new Throwable(s"error occurred: ${e.getMessage}"))
    }
  }
}
