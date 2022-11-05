import workers.LoadFromHDFS

object Main {
  def main(args: Array[String]): Unit = {
    LoadFromHDFS.loadFromHDFS
  }
}
