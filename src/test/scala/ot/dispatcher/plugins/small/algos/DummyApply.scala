package ot.dispatcher.plugins.small.algos

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import ot.dispatcher.plugins.small.SmallApplySpec
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

object DummyApply extends ApplyModel {

  import SmallApplySpec._

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils)
    : DataFrame => DataFrame =

    df => {

      parametersToComplete.success(
        ApplyParameters(
          modelName: String,
          modelConfig: Option[Config],
          searchId: Int,
          featureCols: List[String],
          targetName: Option[String],
          keywords: Map[String, String],
          utils: PluginUtils
        )
      )

      df
    }
}
