package ot.dispatcher.plugins.small.algos.fit

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.{PluginConfig, PluginUtils}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

import scala.util.{Failure, Success, Try}

case class Clustering(fieldsUsed: List[String], dataFrame: DataFrame, properties:Map[String, String], modelName: String, searchId:Int, utils: PluginUtils) extends FitAlgorithm {
  import utils._
  val kRange = getRange("num")

  def makePrediction() = {
    val featuresName = s"__${modelName}_features__"
    val assembler = new VectorAssembler()
      .setInputCols(fieldsUsed.toArray)
      .setOutputCol(featuresName)
    val mDf = assembler.transform(dataFrame)

    val m: KMeansModel = null
    val SilhouetteMinValue: Double = -1.0
    val (maxSilhouette, bestModel, predictedDf) = kRange.foldLeft((SilhouetteMinValue, m, mDf)){ (best, k) =>
      val kmeans = new KMeans().setK(k).setFeaturesCol(featuresName)
      val model = kmeans.fit(mDf)
      val predictions = model.transform(mDf)
      val evaluator = new ClusteringEvaluator().setFeaturesCol(featuresName)
      val silhouette = evaluator.evaluate(predictions)
      if(silhouette >= best._1)
        (silhouette, model, predictions)
      else
        best
    }
    val pipeline = new Pipeline().setStages(Array(assembler, bestModel))
    val pipelineModel = pipeline.fit(dataFrame)

    (pipelineModel, predictedDf)
  }

  def getRange(name: String) = {
    val strInterval = properties.get(name) match {
      case Some(x) => x
      case None => sendError(searchId, s"Required parameter '$name' is no specified")
    }
    Try(strInterval.toInt) match {
      case Success(d) => d to d
      case Failure(e) => {
        val arr = strInterval.split("-")
        Try(arr(0).toInt to arr(1).toInt) match {
          case Success(x) => x
          case Failure(_) => sendError(searchId, s"Value of $name param is not valid")
        }
      }
    }
  }
}
