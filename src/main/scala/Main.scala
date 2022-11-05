import sparkjobs.proto.LoadFromHDFS

object Main {
  def main(args: Array[String]): Unit = {
    println(s"STARTED ${"!"*50}")
    LoadFromHDFS.loadFromHDFS
  }
}
