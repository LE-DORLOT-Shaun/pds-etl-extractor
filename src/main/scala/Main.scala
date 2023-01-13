import executors.Orchestrator.getAndSaveParquet
import helpers.Helper

import scala.util.Success

object Main {
  def main(args: Array[String]): Unit = {
    Helper.parseCommand(args) match {
      case Success(command) => {
        println(s"year: ${command.year}, month: ${command.month}")
        getAndSaveParquet(command.year, command.month)
      }
    }
  }
}
