package ot.dispatcher.plugins.small.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.algos.evaluate.RegressionScore
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.plugins.small.utils.SmallModelsUtils
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}


/**
 *
 * Create command to perform metric calculation
 * @param sq - query to command
 * @param utils -
 */
class SmallScore(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils: PluginUtils, Set("vs", "into", "with")) {
  val smallUtils = new SmallModelsUtils(utils)

  import smallUtils._
  import utils._

  val regressionScores = Set("mse", "rmse", "mae", "mape", "smape", "r2", "r2_adj")

  val (metricName, labelCol) = mainArgs match {
    case Nil => sendError("Comand name is not specified")
    case "score" :: metric :: Nil if regressionScores.contains(metric) => sendError("Label column name is not specified")
    case "score" :: metric :: "vs" :: predict :: Nil => sendError("Label column name is not specified")
    case "score" :: metric :: label :: Nil => (metric, label)
    case "score" :: metric :: label :: arg :: Nil if arg != "vs" => sendError("Syntax error, the preposition 'vs' is required between label and predictions columns")
    case "score" :: "r2_adj" :: label :: "vs" :: predict :: "with" :: Nil => sendError("The number of features in the model not specified")
    case "score" :: "r2_adj" :: label :: "vs" :: predict :: _ => sendError("Syntax error, need to specify the instruction 'with' for the number of features in the model")
    case _ :: _ :: t => sendError(s"Syntax error, unknown args: ${t.mkString(", ", "[", "]")}")
  }

  val predictionCol: List[String] = getPositional("vs").getOrElse(List()).map(_.stripBackticks)
  if (predictionCol==Nil) {
    sendError("Prediction column name is not specified")
  }
  val featuresNumber: Double = metricName match {
    case "r2_adj" => getPositional("with").getOrElse(List()).map(_.stripBackticks().toDouble).head
    case _ => 0
  }

  /**
   * Make transform to given dataframe
   * @param _df - given dataframe
   * @return dataframe with the name of the metric and its value
   */
  def transform(_df: DataFrame): DataFrame = {
    val scoreAlgorithm = metricName match{
      case "mse" =>
        log.debug("Running MSE scoring")
        RegressionScore(labelCol, predictionCol, _df, "mse", featuresNumber, sq.searchId)
      case "rmse" =>
        log.debug("Running RMSE scoring")
        RegressionScore(labelCol, predictionCol, _df, "rmse", featuresNumber, sq.searchId)
      case "mae" =>
        log.debug("Running MAE scoring")
        RegressionScore(labelCol, predictionCol, _df, "mae", featuresNumber, sq.searchId)
      case "mape" =>
        log.debug("Running MAPE scoring")
        RegressionScore(labelCol, predictionCol, _df, "mape", featuresNumber, sq.searchId)
      case "smape" =>
        log.debug("Runnign SMAPE scoring")
        RegressionScore(labelCol, predictionCol, _df, "smape", featuresNumber, sq.searchId)
      case "r2" =>
        log.debug("Running R2 scoring")
        RegressionScore(labelCol, predictionCol, _df, "r2", featuresNumber,  sq.searchId)
      case "r2_adj" =>
        log.debug("Running R2_adj scoring")
        RegressionScore(labelCol, predictionCol, _df, "r2_adj", featuresNumber, sq.searchId)
      case x => sendError(s" Metric with name '$x' is unsupported at this moment")
    }
    val res = scoreAlgorithm.makeEvaluate()
    val serviceCols =  res.columns.filter(_.matches("__.*__"))
    res.show()
    res.drop(serviceCols : _*)
  }
}
