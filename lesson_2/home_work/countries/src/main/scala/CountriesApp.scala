import scala.io.Source

object CountriesApp extends App {
  def source = Source.fromURL(
    "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
  )
}
