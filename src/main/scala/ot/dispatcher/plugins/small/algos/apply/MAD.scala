package ot.dispatcher.plugins.small.algos.apply

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import ot.dispatcher.sdk.PluginUtils
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.small.sdk.ApplyModel

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class MAD(fieldsUsed: List[String], properties:Map[String, String], searchId:Int, utils: PluginUtils) extends ApplyAlgorithm {
  import utils._

  override def makePrediction(dataFrame: DataFrame) = {

    def mad(df: DataFrame, fields: List[String]): DataFrame = {
      var result = df
      for (field <- fields) {
        val median = result.stat.approxQuantile(Array(field), Array(0.5), 0)(0)(0)
        result = result.withColumn(field+"-median", result(field)-median)
        val mad = result.stat.approxQuantile(Array(field+"-median"), Array(0.5), 0)(0)(0)
        result = result.withColumn(field+"_mad", lit(mad))
      }
      result
    }

    val predictedDf = mad(dataFrame, fieldsUsed)
    predictedDf
  }


}

object MAD extends ApplyModel {
  override def apply(searchId: Int, featureCols: List[String], targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    val model = MAD(featureCols, keywords, searchId, utils)
    model.makePrediction
  }
}