package ot.dispatcher.plugins.small.algos.fit

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils

import scala.util.{Failure, Success, Try}

case class GradientBoosting(featureCols: List[String], targetCol: String, dataFrame: DataFrame, modelName: String, keywords: Map[String,String], searchId:Int, utils: PluginUtils) extends FitAlgorithm {
  import utils._
  def createPipeline() = {
    val max = keywords.get("max") match{
      case Some(m) => Try(m.toInt) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'max' should be of int type")
      }
      case None => 4
    }

    val num = keywords.get("num") match{
      case Some(m) => Try(m.toInt) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'num' should be of int type")
      }
      case None => 10
    }

    val indexedLabel = s"__indexed${targetCol}__"
    val labelIndexer = new StringIndexer()
      .setInputCol(targetCol)
      .setOutputCol(indexedLabel)
      .fit(dataFrame)
    labelIndexer.transform(dataFrame).show

    val featuresName = s"__${modelName}_features__"
    val featureAssembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol(featuresName)

    val afdf = featureAssembler.transform(dataFrame)
    val indexedFeatures = s"__indexed${featuresName}"
    val featureIndexer = new VectorIndexer()
      .setInputCol(featuresName)
      .setOutputCol(indexedFeatures)
      .setMaxCategories(max)
      .fit(afdf)
    featureIndexer.transform(afdf).show()

    val predictionName = s"__${modelName}_prediction__"
    val rf = new GBTClassifier()
      .setLabelCol(indexedLabel)
      .setFeaturesCol(indexedFeatures)
      .setPredictionCol(predictionName)
      .setRawPredictionCol("raw_prediction")
      .setProbabilityCol("probability_prediction")
      //.setNumTrees(num)

    val labelConverter = new IndexToString()
      .setInputCol(predictionName)
      .setOutputCol(modelName + "_prediction")
      .setLabels(labelIndexer.labels)

    new Pipeline().setStages(Array(labelIndexer, featureAssembler, featureIndexer, rf, labelConverter))
  }

  def makePrediction(): (PipelineModel, DataFrame) = {
    val p = createPipeline()
    val pModel = p.fit(dataFrame)
    val rdf = pModel.transform(dataFrame)
    (pModel, rdf)
  }

}

