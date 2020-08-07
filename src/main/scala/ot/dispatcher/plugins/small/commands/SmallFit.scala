package ot.dispatcher.plugins.small.commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.NumericType
import ot.dispatcher.plugins.small.algos.fit.{Classification, Clustering, GradientBoosting, LinearRegression, RandomForest}
import ot.dispatcher.plugins.small.utils.SmallModelsUtils
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}


class SmallFit(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils: PluginUtils, Set("from", "into")) {
  val smallUtils = new SmallModelsUtils(utils)
  import smallUtils._
  val supervisedAlgos = Set("regression", "classification", "random_forest", "gradient_boosting")
  val featureCols = getPositional("from").getOrElse(List()).map(_.stripBackticks())
  val (algoname, targetCol) = mainArgs match {
    case Nil => sendError("Algorithm name is not specified")
    case h::Nil if supervisedAlgos.contains(h) => sendError("Target column name is not specified")
    case h::Nil => (h, null)
    case h0::h1::Nil => (h0, h1)
    case _::_::t =>  sendError(s"Sintax error, unknown args: ${t.mkString(", ", "[", "]")}")
  }
  val modelName = getPositional("into").getOrElse(List()).headOption
    .getOrElse(algoname + "-" + sq.searchId).stripBackticks()

  def transform(_df: DataFrame): DataFrame = {
    val nonNumericFeature =_df.schema.find(f => featureCols.contains(f.name) && targetCol!=f.name && !f.dataType.isInstanceOf[NumericType])
    nonNumericFeature.map {x => sendError( s" Feature column '${x.name}' have non numeric type")}

      val model = algoname match{
      case "regression" =>
        log.debug("Running regression")
        LinearRegression(featureCols, targetCol, _df, modelName, sq.searchId)
      case "clustering" =>
        Clustering(featureCols, _df, getKeywords(), modelName, sq.searchId, utils)
      case "classification" =>
        Classification(featureCols, targetCol, _df, modelName, sq.searchId)
      case "random_forest" =>
        RandomForest(featureCols, targetCol, _df, modelName, getKeywords, sq.searchId, utils)
      case "gradient_boosting" =>
        GradientBoosting(featureCols, targetCol, _df, modelName, getKeywords, sq.searchId, utils)
      case x => sendError(s" Algorithm with name '$x'  is unsupported at this moment")
    }

    val (pModel, res) = model.makePrediction()
    toCache(pModel, modelName, sq.searchId)
    val serviceCols =  res.columns.filter(_.matches("__.*__"))
    res.drop(serviceCols : _*)
  }
}
