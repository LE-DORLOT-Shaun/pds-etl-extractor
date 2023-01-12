import executors.Orchestrator.getAndSaveParquet
import sparkjobs.proto.LoadFromHDFS

object Main {
  def main(args: Array[String]): Unit = {
    getAndSaveParquet("2022", "01")
  }
}
