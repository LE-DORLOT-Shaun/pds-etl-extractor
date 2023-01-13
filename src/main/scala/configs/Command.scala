package configs


class Command {
  var year : String = ""
  var month : String = ""

  def getYear: String = {
    year
  }

  def getMonth: String = {
    month
  }

  def setYear(newYear : String): Unit = {
    year = newYear
  }

  def setMonth(newMonth : String): Unit = {
    month = newMonth
  }
}

