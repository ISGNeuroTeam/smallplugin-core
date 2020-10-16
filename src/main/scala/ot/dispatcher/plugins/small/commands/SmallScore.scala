package ot.dispatcher.plugins.small.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
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

  private val (metricName, labelCol) = returns.flatFields.map(_.stripBackticks()) match {
    case "score" :: first :: second :: _ => (first, second)
    case "score" :: first :: _ => sendError("Label column name is not specified")
    case _ => sendError("Metric is not specified")
  }

  private val predictionCol: String = getPositional("vs").getOrElse(List()).map(_.stripBackticks) match {
    case head :: _ => head.toString()
    case _ => sendError("Prediction column name is not specified")
  }

  /**
   * Make transform to given dataframe
   * @param df - given dataframe
   * @return dataframe with the name of the metric and its value
   */
  def transform(df: DataFrame): DataFrame = {

    // 1. Get algorithm details reader
    val configReader: String => Try[String] =
      getAlgorithmParameters(pluginConfig, "score")

    // 2. Prepare algorithm config loader
    val loadAlgorithmConfig: String => Try[Config] =
      algorithmConfigLoader(pluginConfig)

    // 3. Get algorithm details by name, or default
    // 4. Load algorithm config
    val algorithmDetails: Try[(Option[Config], String)] =
    configReader(metricName)
      .orElse(configReader("default"))
      .flatMap(parseAlgorithmParameters)
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
          keywords = getKeywords(),
          utils = utils
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
