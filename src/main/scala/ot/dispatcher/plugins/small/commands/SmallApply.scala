package ot.dispatcher.plugins.small.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.algos.apply.{Anomaly, IQR, MAD, LocalOutlierFactor, Predict, SavedModel, ZScore}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._



class SmallApply(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from")) {
  val featureCols = getPositional("from").getOrElse(List()).map(_.stripBackticks())
  val algoname = mainArgs match {
    case Nil => sendError("Algorithm or model name is not specified")
    case h::_ => h
  }
  def getTargetName() = mainArgs.lift(1) match {
        case Some(x) => x.stripBackticks()
        case None => sendError("Value column name is not specified")
  }

  def transform(_df: DataFrame): DataFrame = {
    val algo = algoname match {
      case "predict" => Predict(getTargetName(), getKeywords(), sq.searchId, utils)
      case "anomaly" => Anomaly(getTargetName(), getKeywords(), sq.searchId, utils)
      case "lof" => LocalOutlierFactor(featureCols, getKeywords(), sq.searchId, utils)
      case "iqr" => IQR(featureCols, getKeywords(), sq.searchId, utils)
      case "zscore" => ZScore(featureCols, getKeywords(), sq.searchId, utils)
      case "mad" => MAD(featureCols, getKeywords(), sq.searchId, utils)

      case _ => SavedModel(algoname, sq.searchId, utils)
    }
    val res = algo.makePrediction(_df)
    val serviceCols =  res.columns.filter(_.matches("__.*__"))
    res.drop(serviceCols : _*)
    res
  }
}
