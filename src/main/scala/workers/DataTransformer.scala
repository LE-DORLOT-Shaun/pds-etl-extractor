package workers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

    val df_str = df
      .withColumn("start_time", col("start_time").cast("String"))
      .withColumn("end_time", col("end_time").cast("String"))

    import sparkSession.implicits._
    val ds = df_str.map(row => {
      val roomId = row.getLong(0)

      val start_date = transformTime(row.getString(1))

      var end_date = row.getString(2)

      if (start_date != row.getString(1)) {
        end_date = transformTime(end_date)
      }
      val nb_persons = row.getDouble(3) * row.getDouble(4)

      (roomId, start_date, end_date, nb_persons.toInt)
    })

    val df_cleaned = ds.toDF("RoomId", "start_date", "end_date", "nb_persons")

    df_cleaned.show(20)

    try {
      println(s"${"-" * 25} SAVING FILE STARTED ${"-" * 25}")

      // Write to final
      df_cleaned.checkpoint(true)
        .write
        .mode(SaveMode.Append)
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
    // Dates
    val date = date_time(0)
    val time = date_time(1)

    // Time
    val time_arr = time.split(':')
    var hour = time_arr(0).toLong
    val minutes = time_arr(1).toLong
    val seconds = time_arr(2)

    // Transformation
    if(hour < 7) {
      hour = hour + 12
    }else if (hour > 19) {
      hour = hour - 12
    }

    s"${date} ${hour}:${minutes}:${seconds}"

  }

}
