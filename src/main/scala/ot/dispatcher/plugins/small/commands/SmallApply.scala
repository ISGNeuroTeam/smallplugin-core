package ot.dispatcher.plugins.small.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.algos.apply.{Anomaly, IQR, LocalOutlierFactor, MAD, Predict, SavedModel, ZScore}
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._

import scala.util.{Failure, Success, Try}



class SmallApply(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from")) {
  private val featureCols = getPositional("from").getOrElse(List()).map(_.stripBackticks())
  private val algoname = mainArgs match {
    case Nil => sendError("Algorithm or model name is not specified")
    case h::_ => h
  }
  private lazy val targetName: Option[String] =
    mainArgs.lift(1)
      .map(_.stripBackticks())


  def transform(_df: DataFrame): DataFrame = {
//    val algo = algoname match {
//      case "predict" => Predict(getTargetName(), getKeywords(), sq.searchId, utils)
//      case "anomaly" => Anomaly(getTargetName(), getKeywords(), sq.searchId, utils)
//      case "lof" => LocalOutlierFactor(featureCols, getKeywords(), sq.searchId, utils)
//      case "iqr" => IQR(featureCols, getKeywords(), sq.searchId, utils)
//      case "zscore" => ZScore(featureCols, getKeywords(), sq.searchId, utils)
//      case "mad" => MAD(featureCols, getKeywords(), sq.searchId, utils)

//      case _ => SavedModel(algoname, sq.searchId, utils)
//    }

    val classLoader = utils.spark.getClass.getClassLoader

    val transformer: Try[DataFrame => DataFrame] = getAlgorithmClassName("apply", algoname)(pluginConfig)
      .flatMap(getModelInstance[ApplyModel](classLoader))
      .map( model =>
        model.apply(
          searchId = sq.searchId,
          featureCols = featureCols,
          targetName = targetName,
          keywords = getKeywords(),
          utils = utils
        )
      )
      .orElse(
        Try(
          SavedModel(algoname, sq.searchId, utils).makePrediction
        )
      )

    val result = transformer
      .map(_(_df))
      .map(res => {
        val serviceCols =  res.columns.filter(_.matches("__.*__"))
        res.drop(serviceCols : _*)
        res
      })

    result match {
      case Success(df) =>
        df
      case Failure(exception) =>
        sendError(s"Can not get instance of model $algoname to apply. ${exception.getMessage}")
    }
  }

}
