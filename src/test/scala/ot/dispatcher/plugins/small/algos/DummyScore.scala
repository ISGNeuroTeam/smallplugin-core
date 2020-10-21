package ot.dispatcher.plugins.small.algos

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import ot.dispatcher.plugins.small.SmallScoreSpec
import ot.dispatcher.plugins.small.sdk.ScoreModel
import ot.dispatcher.sdk.PluginUtils

object DummyScore extends ScoreModel{

  import SmallScoreSpec._

  override def score(modelName: String, modelConfig: Option[Config], searchId: Int, labelCol: String,
                     predictionCol: String, keywords: Map[String, String], utils: PluginUtils)
    : DataFrame => DataFrame =

    df => {

      parametersToComplete.success(
        ScoreParameters(
          modelName = modelName,
          modelConfig = modelConfig,
          searchId = searchId,
          labelCol = labelCol,
          predictionCol = predictionCol,
          keywords = keywords,
          utils = utils
        )
      )

      df
    }
}
