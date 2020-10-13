package ot.dispatcher.plugins.small.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.algos.evaluate.RegressionScore
import ot.dispatcher.plugins.small.sdk.ScoreModel
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.plugins.small.utils.SmallModelsUtils
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import scala.util.{Failure, Success, Try}


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

  private val regressionScores = Set("mse", "rmse", "mae", "mape", "smape", "r2", "r2_adj")

  private val (metricName, labelCol) = mainArgs match {
    case Nil => sendError("Comand name is not specified")
    case "score" :: metric :: Nil if regressionScores.contains(metric) => sendError("Label column name is not specified")
    case "score" :: metric :: "vs" :: predict :: Nil => sendError("Label column name is not specified")
    case "score" :: metric :: label :: Nil => (metric, label)
    case "score" :: metric :: label :: arg :: Nil if arg != "vs" => sendError("Syntax error, the preposition 'vs' is required between label and predictions columns")
    case "score" :: "r2_adj" :: label :: "vs" :: predict :: "with" :: Nil => sendError("The number of features in the model not specified")
    case "score" :: "r2_adj" :: label :: "vs" :: predict :: _ => sendError("Syntax error, need to specify the instruction 'with' for the number of features in the model")
    case _ :: _ :: t => sendError(s"Syntax error, unknown args: ${t.mkString(", ", "[", "]")}")
  }

  private val predictionCol: List[String] = getPositional("vs").getOrElse(List()).map(_.stripBackticks)
  if (predictionCol==Nil) {
    sendError("Prediction column name is not specified")
  }
  private val featuresNumber: Double = metricName match {
    case "r2_adj" => getPositional("with").getOrElse(List()).map(_.stripBackticks().toDouble).head
    case _ => 0
  }

  /**
   * Make transform to given dataframe
   * @param df - given dataframe
   * @return dataframe with the name of the metric and its value
   */
  def transform(df: DataFrame): DataFrame = {

    // 1. Get algorithm details reader
    val configReader: String => Try[String] =
      getAlgorithmClassName(pluginConfig, "score")

    // 2. Prepare algorithm config loader
    val loadAlgorithmConfig: String => Try[Config] =
      algorithmConfigLoader("baseDir")

    // 3. Get algorithm details by name, or default
    // 4. Load algorithm config
    val algorithmDetails: Try[(Option[Config], String)] =
    configReader(metricName)
      .orElse(configReader("default"))
      .flatMap(getAlgorithmDetails)
      .flatMap { case (algorithmConfigName, algorithmClassName) =>
        algorithmConfigName
          .map(loadAlgorithmConfig)
          .map(_.map(cfg => (Some(cfg), algorithmClassName)))
          .getOrElse(Success((Option.empty[Config], algorithmClassName)))
      }


    val classLoader = utils.spark.getClass.getClassLoader

    val configAndModel: Try[(Option[Config], ScoreModel)] =  algorithmDetails
      .flatMap { case (cfg, className) =>
        getModelInstance[ScoreModel](classLoader)(className)
          .map(model => (cfg, model))
      }


    val result = configAndModel
      .map { case (cfg, model) =>
        model.score(
          modelName = metricName,
          modelConfig = cfg,
          searchId = sq.searchId,
          labelCol = labelCol,
          predictionCol = predictionCol,
          metricName = metricName,
          featuresNumber = featuresNumber
        )
      }
      .map(algo => {
        val res = algo(df)
        val serviceCols =  res.columns.filter(_.matches("__.*__"))
        res.show()
        res.drop(serviceCols : _*)
      })

    result match {
      case Success(dataFrame) =>
        dataFrame
      case Failure(exception) =>
        sendError(s"Can not get metric '$metricName' to score. ${exception.getMessage}")
    }
  }
}
