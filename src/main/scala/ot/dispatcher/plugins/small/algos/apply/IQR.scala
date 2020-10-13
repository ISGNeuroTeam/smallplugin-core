package ot.dispatcher.plugins.small.algos.apply

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame}
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

import scala.util.{Failure, Success, Try}

case class IQR(fieldsUsed: List[String], properties:Map[String, String], searchId:Int, utils: PluginUtils) {
  import utils._

  def makePrediction(dataFrame: DataFrame) = {

    val List(q1, q3) = properties.get("range") match {
      case Some(m) => m.filterNot(c => c  == '(' || c == ')' || c == ' ').split(",").map(_.toDouble).toList
      case None => List[Double](0.25,0.75)
    }
    val withCentering = properties.get("with_centering") match {
      case Some(m) => Try(m.toBoolean) match {
        case Success(x) => x
        case Failure(_) => sendError(searchId, "The value of parameter 'with_centering' should be of boolean type")
      }
      case None => true
    }
    val withScaling = properties.get("with_scaling") match {
      case Some(m) => Try(m.toBoolean) match {
        case Success(x) => x
        case Failure(_) => sendError(searchId, "The value of parameter 'with_scaling' should be of boolean type")
      }
      case None => true
    }

    def get_iqr(q1: Double, q2: Double, q3: Double)(x: Column): Column = {
      var result=x
      if (withCentering) result = x-q2
      if (withScaling) result = result/(q3-q1)
      result
    }

    def quantile(df: DataFrame, fields: List[String], q: Array[Double]): DataFrame = {
      var result = df
      for (field <- fields) {
        val Array(q1, q2, q3) = df.stat.approxQuantile(Array(field), q, 0)(0)
        result = result.withColumn("iqr_" + field, get_iqr(q1, q2, q3)(result(field)))
      }
      result
    }

    val predictedDf = quantile(dataFrame, fieldsUsed, Array(q1, 0.5, q3))
    predictedDf
  }


}

object IQR extends ApplyModel {
  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String], targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    val model = IQR(featureCols, keywords, searchId, utils)
    model.makePrediction
  }
}