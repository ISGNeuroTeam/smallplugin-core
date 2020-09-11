package ot.dispatcher.plugins.small.algos.apply

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import org.apache.spark.ml.outlier._
import org.apache.spark.ml.feature.VectorAssembler

import scala.util.{Failure, Success, Try}

case class LocalOutlierFactor(featureCols: List[String], keywords: Map[String, String], id: Int, utils: PluginUtils) extends ApplyAlgorithm{
  import utils._
  override def makePrediction(df: DataFrame): DataFrame = {
    val DefaultMinPts = 5
//    val DefaultDistType = "euclidean"

    val minPts = keywords.get("min_pts") match {
      case Some(x) => Try(x.toInt) match {
        case Success(i) => i
        case Failure(_) => sendError(id, "The value of parameter 'minPts' should be of int type")
      }
      case None => DefaultMinPts
    }

//    val distType = keywords.get("dist_type") match {
//      case Some(x) => x
//      case None => DefaultDistType
//    }

    val featuresName = s"features"
    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol(featuresName)

    val data = assembler.transform(df).repartition(4)

    val result = new LOF()
      .setMinPts(minPts)
      .transform(data)

    result
  }
}
