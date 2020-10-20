package ot.dispatcher.plugins.small.algos

import com.typesafe.config.Config

import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.DataFrame

import ot.dispatcher.plugins.small.{SmallFitSpec, SmallFitTest}
import ot.dispatcher.plugins.small.sdk.FitModel
import ot.dispatcher.sdk.PluginUtils


object DummyFit extends FitModel {

  import ot.dispatcher.plugins.small.SmallFitSpec.{FitParameters, parametersToComplete}


  override def fit(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                   targetCol: Option[String], keywords: Map[String, String], utils: PluginUtils)
    : DataFrame => (PipelineModel, DataFrame) =

    df => {

      parametersToComplete.success(
        FitParameters(
          modelName = modelName,
          modelConfig = modelConfig,
          searchId = searchId,
          featureCols = featureCols,
          targetCol = targetCol,
          keywords = keywords,
          utils = utils
        )
      )

      val stage: Transformer =
        new SQLTransformer("dummy")
          .setStatement("SELECT * FROM __THIS__")

      val pm: PipelineModel =
        new Pipeline("dummy")
          .setStages(Array(stage))
          .fit(df)
      (pm, df)
    }
}
