package helpers

import configs.{ArgConfig, Command, ConfigElement, DataFileConfig}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import scopt.{OParser, OParserBuilder}
import spray.json._

import scala.util.{Failure, Success, Try}

object Helper {

  // Scopt Parser
  val builder: OParserBuilder[ArgConfig] = OParser.builder[ArgConfig]

  val parser: OParser[Unit, ArgConfig] = {
    import builder._
    OParser.sequence(
      programName("compliance-system"),
      head("compliance-system", "1.0"),
      opt[String]('y', "year")
        .valueName("<year>")
        .action((x, c) => c.copy(year = x))
        .text("year is required if not read"),

      opt[String]('m', "month")
        .valueName("<month>")
        .action((x, c) => c.copy(month = x))
        .text("month is required if not read")
    )
  }

  def parseCommand(args: Array[String]): Try[Command] = {
    val command = new Command()

    OParser.parse(parser, args, ArgConfig()) match {
      case Some(config) =>
        command.setYear(config.year)
        command.setMonth(config.month)
        Success(command)
      case _ =>
        Failure(new Throwable("program args invalid"))
    }
  }

  def sparkSchemeFromJSON(): StructType = {
    // Basic type mapping map
    val stringToType = Map[String, DataType](
      "String" -> StringType,
      "Double" -> DoubleType,
      "Float" -> FloatType,
      "Int" -> IntegerType,
      "Boolean" -> BooleanType,
      "Long" -> LongType,
      "DateTime" -> DateType
    )

    // Load JSON From file
    val source = scala.io.Source.fromFile("src/confs/DataFileConfig.json")
    val lines = try source.mkString finally source.close()
    val json = lines.parseJson

    // Parse to case class
    import configs.JsonProtocol._
    val datafileConfig = json.convertTo[DataFileConfig[ConfigElement]]

    // Convert case class to StructType
    var structSeq: Seq[StructField] = Seq()
    datafileConfig.columns.foreach(configElement => {
      structSeq = structSeq :+ StructField(configElement.name, stringToType(configElement.typeOf), nullable = false)
    })
    StructType(structSeq)
  }

  def dataframeToDataset(df : DataFrame): Dataset[String] = {
    val encoder = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[String]
    df.as(encoder)
  }
}


