package ot.dispatcher.plugins.small.algos

import com.typesafe.config.Config
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.FitModel
import ot.dispatcher.sdk.PluginUtils

object DummyFit extends FitModel {
  override def fit(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String], targetCol: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => (PipelineModel, DataFrame) =
    ???
}
