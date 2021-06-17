package ot.dispatcher.plugins.small.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.NumericType
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
    case _::_::t =>  sendError(s"Syntax error, unknown args: ${t.mkString(", ", "[", "]")}")
  }
  private val modelName = getPositional("into").getOrElse(List()).headOption
    .getOrElse(algoname + "-" + sq.searchId).stripBackticks()

  def transform(df: DataFrame): DataFrame = {
    val nonNumericFeature =df.schema.find(f => featureCols.contains(f.name) && targetCol!=f.name && !f.dataType.isInstanceOf[NumericType])
    nonNumericFeature.map {x => sendError( s" Feature column '${x.name}' have non numeric type")}

    val colsToFill = if (targetCol != null) {targetCol :: featureCols} else {featureCols}
    val filledDf = fixMissing(df, colsToFill)

    // 1. Get algorithm details reader
    val configReader: String => Try[String] =
      getAlgorithmParameters(pluginConfig, "fit")

    // 2. Prepare algorithm config loader
    val loadAlgorithmConfig: String => Try[Config] =
      algorithmConfigLoader(pluginConfig)

    // 3. Get algorithm details by name, or default
    // 4. Load algorithm config
    val algorithmDetails: Try[(Option[Config], String)] =
    configReader(algoname)
      .orElse(configReader("default"))
      .flatMap(parseAlgorithmParameters)
      .flatMap { case (algorithmConfigName, algorithmClassName) =>
        algorithmConfigName
          .map(loadAlgorithmConfig)
          .map(_.map(cfg => (Some(cfg), algorithmClassName)))
          .getOrElse(Success((Option.empty[Config], algorithmClassName)))
      }


    val classLoader: ClassLoader = Thread.currentThread().getContextClassLoader

    val configAndModel: Try[(Option[Config], FitModel)] =
      algorithmDetails
        .flatMap { case (cfg, className) =>
          getModelInstance[FitModel](classLoader)(className)
            .map(model => (cfg, model))
        }

    val result = configAndModel
      .map { case (cfg, model) =>
        model.fit(
          modelName = modelName,
          modelConfig = cfg,
          searchId = sq.searchId,
          featureCols = featureCols,
          targetCol = Option(targetCol),
          keywords = getKeywords(),
          utils = utils
        )
      }
      .map( algo => {
        val (pModel, res) = algo(filledDf)
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
