package workers

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

    import sparkSession.implicits._
    val ds = df.map(row => {
      val roomId = row.getLong(0)
      val start_date = transformTime(row.getTimestamp(1).toString)
      var end_date = row.getTimestamp(2).toString

      if (start_date != row.getString(1)) {
        end_date = transformTime(end_date)
      }
      val nb_persons = row.getLong(3) * row.getLong(4)

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
    println("Transformation time")
    val date_time = datetime.split(' ')
    // Dates
    val date = date_time(0)
    println(s"Transformation time : date ${date}")
    val time = date_time(1)
    println(s"Transformation time : time ${time}")

    // Time
    val time_arr = time.split(':')
    var hour = time_arr(0).toLong
    println(s"Transformation time : hour ${hour}")
    val minutes = time_arr(1).toLong
    println(s"Transformation time : time ${minutes}")
    val seconds = time_arr(2)
    println(s"Transformation time : time ${seconds}")

    // Transformation
    if(hour < 7) {
      hour = hour + 12
    }else if (hour > 20) {
      hour = hour - 12
    }

    s"${date} ${hour}:${minutes}:${seconds}"

  }

}
