package workers

import org.apache.parquet.format.Util
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import workers.HDFSFileManager.getSparkSession


object DataTransformer {
  val sparkHost : String = "local"
  val hdfsSilverPath : String = "hdfs://192.168.1.2:9000/silver/locaux"

  def transformDataSilver(df : DataFrame): Boolean = {

    val sparkSession: SparkSession = SparkSession
      .builder
      .master(sparkHost)
      .appName("pds-etl-manager")
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._
    val ds = df.map(row => {
      val roomId = row.getInt(0)
      val start_date = transformTime(row.getString(1))
      var end_date = row.getString(2)

      if (start_date != row.getString(1)) {
        end_date = transformTime(end_date)
      }
      val nb_persons = row.getInt(3) * row.getInt(4)

      (roomId, start_date, end_date, nb_persons)
    })

    val df_cleaned = ds.toDF("RoomId", "start_date", "end_date", "nb_persons")

    df_cleaned.show(20)

    try {
      println(s"${"-" * 25} SAVING FILE STARTED ${"-" * 25}")

      // Write to final
      df_cleaned.checkpoint(true)
        .write
        .mode(SaveMode.Overwrite)
        .save(hdfsSilverPath)

      println("Silver Data File Has Been Successfully Written")

      true
    }
    catch {
      case e: Throwable =>
        println(s"error while saving silver data: ${e.getMessage}")
        false
    }
  }

  def transformTime(datetime : String) : String = {
    val date_time = datetime.split(' ')
    println(datetime)
    println(date_time)
    // Dates
    val date = date_time(0)
    val time = date_time(1)
    // Time
    val time_arr = time.split(':')
    var hour = time_arr(0).toInt
    val minutes = time_arr(1).toInt
    val seconds = time_arr(2).toInt

    // Transformation
    if(hour < 7) {
      hour = hour + 12
    }else if (hour > 20) {
      hour = hour - 12
    }

    s"${date} ${hour}:${minutes}:${seconds}"

  }

}
