import scala.collection.immutable
import scala.io.Source

object TennisEtlApp extends App {
  case class Date(year: Int, month: Int, day: Int)
  case class TennisMatch(
                          tourneyName: String,
                          winnerName: String,
                          loserName: String,
                          date: Date)

  def parseHeaders(headers: String) = {
    headers.split(",").zipWithIndex.toMap
  }

  def parseLine(line: String, headers: Map[String, Int]) = {
    val tokens = line.split(",")
    val dateString = tokens(headers("tourney_date"))
    val date = Date(dateString.take(4).toInt, dateString.drop(4).take(2).toInt, dateString.drop(6).toInt)
    TennisMatch(
      tokens(headers("tourney_name")),
      tokens(headers("winner_name")),
      tokens(headers("loser_name")),
      date
    )
  }

  def source = Source.fromURL(
    "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_2020.csv"
  )

  val head :: records = source.getLines.toList
  val headers = parseHeaders(head)
  val matches: immutable.Seq[TennisMatch] = records.map(parseLine(_, headers))

  def filterRecords(playerName: String, month: Int)(records: Iterable[TennisMatch]) = {
    records.filter(record =>
      (record.loserName == playerName || record.winnerName == playerName)
        && record.date.month == month
    )
  }

  def renderRecord(playerName: String)(record: TennisMatch) = {
    record match {
      case TennisMatch(tourneyName, winnerName, loserName, _)
        if loserName == playerName => List(tourneyName, winnerName, "L").mkString(",")
      case TennisMatch(tourneyName, winnerName, loserName, _)
        if winnerName == playerName => List(tourneyName, loserName, "W").mkString(",")
    }
  }

//  println(matches.take(5).mkString("\n"))
//  println(filterRecords("Daniil Medvedev", 10)(matches).mkString("\n"))

  val firstRecord = filterRecords("Daniil Medvedev", 10)(matches).head
  val rendered = renderRecord("Daniil Medvedev")(firstRecord)
  println(rendered)
}
