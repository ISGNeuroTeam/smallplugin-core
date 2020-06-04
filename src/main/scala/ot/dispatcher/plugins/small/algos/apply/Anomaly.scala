package ot.dispatcher.plugins.small.algos.apply
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils

import scala.util.{Failure, Success, Try}

case class Anomaly(targetCol: String, keywords: Map[String, String], id: Int, utils: PluginUtils) extends ApplyAlgorithm{
  import utils._
  override def makePrediction(df: DataFrame): DataFrame = {
    val DefaultWindow = 100
    val DefaultTolerance = 3
    val window = keywords.get("window") match {
      case Some(x) => Try(x.toInt) match {
        case Success(i) => i
        case Failure(_) => sendError(id, "The value of parameter 'window' should be of int type")
      }
      case None => DefaultWindow
    }
    val tolerance = keywords.get("tolerance") match {
      case Some(x) => Try(x.toDouble) match {
        case Success(i) => i
        case Failure(_) => sendError(id, "The value of parameter 'tolerance' should be of number type")
      }
      case None => DefaultTolerance
    }

    val query= s"""| streamstats window=$window  stdev($targetCol) as std, avg($targetCol) as av
                   | eval upperBound = av + $tolerance*std
                   | eval lowerBound = av - $tolerance*std
                   | eval isOutlier = if($targetCol > upperBound OR $targetCol < lowerBound, 1, 0)
                   | fields - av, std"""

    executeQuery(query, df)
  }
}
