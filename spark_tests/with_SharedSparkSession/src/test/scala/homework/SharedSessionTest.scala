package homework

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

class SharedSessionTest extends SharedSparkSession {
  import ReadWriteUtils._

  test("Most Popular") {
    val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
    val taxiInfoFile = "src/main/resources/data/taxi_zones.csv"
    val taxiFactsDF = readParquet(taxiFactsFile)
    val taxiInfoDF  = readCSV(taxiInfoFile)

    val mostPopularDF = MostPopular.processTaxiData(taxiFactsDF, taxiInfoDF)

    checkAnswer(
      mostPopularDF,
      Row("Manhattan",304266,66.0) ::
        Row("Queens",17712,53.5) ::
        Row("Unknown",6644,42.8) ::
        Row("Brooklyn",3037,27.37) ::
        Row("Bronx",211,20.09) ::
        Row("EWR",19,17.3) ::
        Row("Staten Island",4,0.5) :: Nil
    )
  }

  test("Order Distribution") {
    val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
    val taxiFactsDF = readParquet(taxiFactsFile)
    val dist = OrderDistribution.processTaxiData(taxiFactsDF)

    checkAnswer(
      dist.sort(col("Hour")),
      Row(0,15348,2.3924459212926736,34.8,0.0,3.091562994698098) ::
        Row(1,15564,2.368352608583927,49.95,0.0,3.081749949534247) ::
        Row(2,16001,2.5794331604274774,37.4,0.0,3.5641394241699946) ::
        Row(3,16082,2.7878074866310025,66.0,0.0,3.9121747538636744) ::
        Row(4,17483,2.8287176113939347,40.32,0.0,3.9975302111539475) ::
        Row(5,17843,2.660396794261058,51.6,0.0,3.668808853156986) ::
        Row(6,16160,2.8585897277227876,43.5,0.0,4.023001549424064) ::
        Row(7,18664,2.53774539219888,37.1,0.0,3.378057299799056) ::
        Row(8,22121,2.4006514172053692,46.3,0.0,3.081200083780239) ::
        Row(9,21598,2.5563334568015557,41.8,0.0,3.289031427065351) ::
        Row(10,20318,2.869695344029931,40.7,0.0,3.5387762425151537) ::
        Row(11,20884,3.0914580540126635,46.51,0.0,3.5516603682052192) ::
        Row(12,19528,3.1231452273658173,42.8,0.0,3.3629271946996266) ::
        Row(13,14652,3.1506872781872897,43.8,0.0,3.392143799872522) ::
        Row(14,7050,3.3324765957446827,51.6,0.0,3.7284342116987745) ::
        Row(15,3978,3.0986626445449956,55.41,0.0,3.3793394172493025) ::
        Row(16,2538,3.125460992907798,32.47,0.0,3.132702248579828) ::
        Row(17,1610,3.4902049689441013,40.78,0.0,3.5986339194187904) ::
        Row(18,1586,4.324142496847414,26.38,0.0,4.583290793831162) ::
        Row(19,3133,4.043013086498559,28.1,0.0,5.117481350784475) ::
        Row(20,8600,2.8695883720930233,29.87,0.0,3.8285114242848275) ::
        Row(21,15445,2.467402395597279,40.5,0.0,3.148808815364656) ::
        Row(22,18867,2.249595060157952,45.98,0.0,2.8646599594460547) ::
        Row(23,16840,2.3423960807601003,53.5,0.0,3.129309511122047) :: Nil
    )
  }

}
