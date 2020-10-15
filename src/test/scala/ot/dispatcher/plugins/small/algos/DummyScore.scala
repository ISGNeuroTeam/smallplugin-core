package ot.dispatcher.plugins.small.algos

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.ScoreModel
import ot.dispatcher.sdk.PluginUtils

object DummyScore extends ScoreModel{
  override def score(modelName: String, modelConfig: Option[Config], searchId: Int, labelCol: String, predictionCol: String, keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame =
    ???
}
