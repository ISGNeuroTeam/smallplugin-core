package ot.dispatcher.plugins.small.algos.apply

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame}
import ot.dispatcher.sdk.PluginUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.small.sdk.ApplyModel

import scala.util.{Failure, Success, Try}

case class ZScore(fieldsUsed: List[String], properties:Map[String, String], searchId:Int, utils: PluginUtils) {
  import utils._

  // columns to group window by
  val by = properties.get("by") match {
    case Some(m) => m.split(",").map(_.trim).toList
    case None => List[String]()
  }
  // num of rows to take before, unbounded in case of None
  val window_before = properties.get("window_before") match{
    case Some(m) => Try(m.toInt) match{
      case Success(n) => -1*n
      case Failure(_) => sendError(searchId, "The value of parameter 'window_before' should be of int type")
    }
    case None => properties.get("window") match {
      case Some(m) => Try(m.toInt) match{
        case Success(n) => -1*n
        case Failure(_) => sendError(searchId, "The value of parameter 'window' should be of int type")
      }
      case None => Window.unboundedPreceding
    }
  }
  // num of rows to take after, unbounded in case of None
  val window_after = properties.get("window_after") match{
    case Some(m) => Try(m.toInt) match{
      case Success(n) => n
      case Failure(_) => sendError(searchId, "The value of parameter 'window_after' should be of int type")
    }
    case None => properties.get("window") match {
      case Some(m) => Try(m.toInt) match{
        case Success(_) => 0
        case Failure(_) => sendError(searchId, "The value of parameter 'window' should be of int type")
      }
      case None => Window.unboundedFollowing
    }
  }
  // make partition by columns "by" in given window
  val byColumns = Window.partitionBy(by.map(col(_)):_*).rowsBetween(window_before,window_after)


  def makePrediction(dataFrame: DataFrame) = {

    def zscore(mean: Column, sd: Column)(x: Column) =
      (x - mean) / sd
    def mean_std_z(df: DataFrame, colsToUse: List[String]): DataFrame = {
      var result = df
      for (colName <- colsToUse) {
        result = result.withColumn("mean_" + colName, avg(colName).over(byColumns))
          .withColumn("stddev_" + colName, stddev(colName).over(byColumns))
        result = result.withColumn("zscore_" + colName, zscore(result("mean_" + colName), result("stddev_" + colName))(result(colName)))
      }
      result
    }
    val predictedDf = mean_std_z(dataFrame, fieldsUsed)
    predictedDf
  }
}

object ZScore extends ApplyModel {
  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String], targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    val model = ZScore(featureCols, keywords, searchId, utils)
    model.makePrediction
  }
}