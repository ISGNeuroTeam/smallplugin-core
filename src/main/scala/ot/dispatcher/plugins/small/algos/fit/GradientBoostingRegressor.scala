package ot.dispatcher.plugins.small.algos.fit

import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils

import scala.util.{Failure, Success, Try}

case class GradientBoostingRegressor(featureCols: List[String], targetCol: String, dataFrame: DataFrame, modelName: String, keywords: Map[String,String], searchId:Int, utils: PluginUtils) extends FitAlgorithm {
  import utils._
  def createPipeline() = {
    val max = keywords.get("max") match{
      case Some(m) => Try(m.toInt) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'max' should be of int type")
      }
      case None => 4
    }

    val maxDepth = keywords.get("maxDepth") match{
      case Some(m) => Try(m.toInt) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'maxDepth' should be of int type")
      }
      case None => 3
    }

    val learningRate = keywords.get("learningRate") match{
      case Some(m) => Try(m.toFloat) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'learningRate' should be of float type")
      }
      case None => 0.1
    }

    val iterationSubsample = keywords.get("iterationSubsample") match{
      case Some(m) => Try(m.toFloat) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'iterationSubsample' should be of float type")
      }
      case None => 1
    }

    val minInfoGain = keywords.get("minInfoGain") match{
      case Some(m) => Try(m.toFloat) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'minInfoGain' should be of float type")
      }
      case None => 0.0
    }

    val minLeafSamples = keywords.get("minLeafSamples") match{
      case Some(m) => Try(m.toInt) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'minLeafSamples' should be of int type")
      }
      case None => 1
    }

    val numTrees = keywords.get("numTrees") match{
      case Some(m) => Try(m.toInt) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'numTrees' should be of int type")
      }
      case None => 100
    }

    val maxBins = keywords.get("maxBins") match{
      case Some(m) => Try(m.toInt) match{
        case Success(n) => n
        case Failure(_) => sendError(searchId, "The value of parameter 'maxBins' should be of int type")
      }
      case None => 32
    }

    val subsetStrategy = keywords.get("subsetStrategy") match{
      case Some(m) => Try(m.toString) match{
        case Success("auto")  => "auto"
        case Success("all")  => "all"
        case Success("onethird")  => "onethird"
        case Success("sqrt")  => "sqrt"
        case Success("log2")  => "log2"
        case Success(n) => sendError(searchId, "No such strategy. Available strategies: auto, all, onethird, sqrt, log2")
        case Failure(_) => sendError(searchId, "The value of parameter 'subsetStrategy' should be of string type")
      }
      case None => "auto"
    }

    val lossType = keywords.get("lossType") match{
      case Some(m) => Try(m.toString) match{
        case Success("squared")  => "squared"
        case Success("absolute")  => "absolute"
        case Success(n) => sendError(searchId, "No such loss type. Available types: squared, absolute")
        case Failure(_) => sendError(searchId, "The value of parameter 'lossType' should be of string type")
      }
      case None => "squared"
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
    val gbt = new GBTRegressor()
      .setLabelCol(indexedLabel)
      .setFeaturesCol(indexedFeatures)
      .setPredictionCol(predictionName)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minLeafSamples)
      .setStepSize(learningRate)
      .setMaxDepth(maxDepth)
      .setSubsamplingRate(iterationSubsample)
      .setMaxIter(numTrees)
      .setMaxBins(maxBins)
      .setFeatureSubsetStrategy(subsetStrategy)
      .setLossType(lossType)


    val labelConverter = new IndexToString()
      .setInputCol(predictionName)
      .setOutputCol(modelName + "_prediction")
      .setLabels(labelIndexer.labels)

    new Pipeline().setStages(Array(labelIndexer, featureAssembler, featureIndexer, gbt, labelConverter))
  }

  def makePrediction(): (PipelineModel, DataFrame) = {
    val p = createPipeline()
    val pModel = p.fit(dataFrame)
    val rdf = pModel.transform(dataFrame)
    (pModel, rdf)
  }

}

