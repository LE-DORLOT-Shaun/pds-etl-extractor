import executors.Orchestrator.getAndSaveParquet

object Main {
  def main(args: Array[String]): Unit = {
    getAndSaveParquet("2022", "01")
  }
}
