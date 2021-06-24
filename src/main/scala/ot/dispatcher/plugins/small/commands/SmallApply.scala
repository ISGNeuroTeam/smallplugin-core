package ot.dispatcher.plugins.small.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.plugins.small.utils.SmallModelsUtils

import scala.util.{Failure, Success, Try}



class SmallApply(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from")) {
  private val smallUtils = new SmallModelsUtils(utils)
  import smallUtils._

  private val featureCols = getPositional("from").getOrElse(List()).map(_.stripBackticks())
  private val algoname = mainArgs match {
    case Nil => sendError("Algorithm or model name is not specified")
    case h::_ => h
  }
  private lazy val targetName: Option[String] =
    mainArgs.lift(1)
      .map(_.stripBackticks())


  def transform(df: DataFrame): DataFrame = {

    val filledDf = fixMissing(df, featureCols)

    // 1. Get algorithm details reader
    val configReader: String => Try[String] =
      getAlgorithmParameters(pluginConfig, "apply")

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


    val classLoader = Thread.currentThread().getContextClassLoader

    val transformer: Try[DataFrame => DataFrame] =
      algorithmDetails
        .flatMap { case (cfg, className) =>
          getModelInstance[ApplyModel](classLoader)(className)
            .map(model => (cfg, model))
        }
        .map { case (cfg, model) =>
          model.apply(
            modelName = algoname,
            modelConfig = cfg,
            searchId = sq.searchId,
            featureCols = featureCols,
            targetName = targetName,
            keywords = getKeywords(),
            utils = utils
          )
        }

    val result = transformer
      .map(_(filledDf))
      .map(res => {
        val serviceCols =  res.columns.filter(_.matches("__.*__"))
        res.drop(serviceCols : _*)
      })

    result match {
      case Success(df) =>
        df
      case Failure(exception) =>
        sendError(s"Can not get instance of model $algoname to apply. ${exception.getMessage}")
    }
  }

}
