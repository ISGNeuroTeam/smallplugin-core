package ot.dispatcher.plugins.small.commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.NumericType
import ot.dispatcher.plugins.small.algos.fit._
import ot.dispatcher.plugins.small.sdk.FitModel
import ot.dispatcher.plugins.small.utils.SmallModelsUtils
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import scala.util.{Failure, Success, Try}


class SmallFit(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils: PluginUtils, Set("from", "into")) {
  private val smallUtils = new SmallModelsUtils(utils)
  import smallUtils._
  private val supervisedAlgos = Set("regression", "classification", "random_forest", "classification_gradient_boosting", "regression_gradient_boosting", "classification_random_forest", "regression_random_forest")
  private val featureCols = getPositional("from").getOrElse(List()).map(_.stripBackticks())
  private val (algoname, targetCol) = mainArgs match {
    case Nil => sendError("Algorithm name is not specified")
    case h::Nil if supervisedAlgos.contains(h) => sendError("Target column name is not specified")
    case h::Nil => (h, null)
    case h0::h1::Nil => (h0, h1)
    case _::_::t =>  sendError(s"Sintax error, unknown args: ${t.mkString(", ", "[", "]")}")
  }
  private val modelName = getPositional("into").getOrElse(List()).headOption
    .getOrElse(algoname + "-" + sq.searchId).stripBackticks()

  def transform(_df: DataFrame): DataFrame = {
    val nonNumericFeature =_df.schema.find(f => featureCols.contains(f.name) && targetCol!=f.name && !f.dataType.isInstanceOf[NumericType])
    nonNumericFeature.map {x => sendError( s" Feature column '${x.name}' have non numeric type")}

//      val model1 = algoname match{
//      case "regression" =>
//        log.debug("Running regression")
//        LinearRegression(featureCols, targetCol, _df, modelName, sq.searchId)
//      case "clustering" =>
//        Clustering(featureCols, _df, getKeywords(), modelName, sq.searchId, utils)
//      case "classification"|"classifier_logreg" =>
//        Classification(featureCols, targetCol, _df, modelName, sq.searchId)
//      case "classification_random_forest"|"classifier_rf"|"class_rf" =>
//        RandomForestClassifier(featureCols, targetCol, _df, modelName, getKeywords, sq.searchId, utils)
//      case "regression_random_forest"|"regressor_rf"|"reg_rf" =>
//        RandomForestRegresor(featureCols, targetCol, _df, modelName, getKeywords, sq.searchId, utils)
//      case "classification_gradient_boosting"|"class_gb"|"class_GradientBoosting"|"classifier_gb" =>
//        GradientBoostingClassifier(featureCols, targetCol, _df, modelName, getKeywords, sq.searchId, utils)
//      case "regression_gradient_boosting"|"reg_gb"|"reg_GradientBoosting"|"regression_gb" =>
//        GradientBoostingRegressor(featureCols, targetCol, _df, modelName, getKeywords, sq.searchId, utils)
//    }

    val classLoader: ClassLoader = utils.spark.getClass.getClassLoader

    val model: Try[FitModel] = getAlgorithmClassName("fit", algoname)(utils.pluginConfig)
      .flatMap(getModelInstance[FitModel](classLoader))
      .orElse(
        Try(sendError(s" Algorithm with name '$algoname'  is unsupported at this moment"))
      )

    val result = model
      .map(
        _.fit(
          modelName = modelName,
          searchId = sq.searchId,
          featureCols = featureCols,
          targetCol = Some(targetCol),
          keywords = getKeywords(),
          utils = utils
        )
      )
      .map( algo => {
        val (pModel, res) = algo(_df)
        toCache(pModel, modelName, sq.searchId)
        val serviceCols = res.columns.filter(_.matches("__.*__"))
        res.drop(serviceCols: _*)
      }
    )

    result match {
      case Success(dataFrame) =>
        dataFrame
      case Failure(exception) =>
        sendError(s"Can not get '$algoname' to fit. ${exception.getMessage}")
    }
  }
}
