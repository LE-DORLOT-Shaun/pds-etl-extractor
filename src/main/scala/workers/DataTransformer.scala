package workers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}


object DataTransformer {

  val hdfsSilverPath : String = "hdfs://192.168.1.2:9000/silver/locaux"

  def transformDataSilver(df : DataFrame): Boolean = {
    val df_cleaned = df
      .withColumn("start_date", col(transformTime(col("start_date").toString())).otherwise(col("start_date")))
      .withColumn("end_date", col(transformTime(col("end_date").toString())).otherwise(col("end_date")))
      .withColumn("nb_persons", col("nb_persons") * col("mult_factor").cast("Integer"))

    df_cleaned.drop("mult_factor")

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
