package ot.dispatcher.plugins.small.algos.score.regression

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.ScoreModel
import ot.dispatcher.sdk.PluginUtils

/**
 * Evaluator for MAPE metric
 * @param labelCol - observation column
 * @param predictionCol - prediction column
 * @param df - dataFrame with two double columns: prediction and observation
 * @param keywords - pairs of key-value
 * @param searchId - search ID
 */
case class MAPE(labelCol: String, predictionCol: String, df: DataFrame, keywords: Map[String, String], searchId: Int) {

  /**
   * Performs MAPE metric calculation
   *
   * @return dataframe with one column: metricName with metric value
   */
  def makeEvaluate(): DataFrame = {
    df.withColumn("error", abs((df(labelCol) - df(predictionCol)) / df(labelCol)))
      .select("error")
      .agg(avg("error") as "MAPE")
  }
}

object MAPE extends ScoreModel {
  override def score(modelName: String,
                     modelConfig: Option[Config],
                     searchId: Int,
                     labelCol: String,
                     predictionCol: String,
                     keywords: Map[String, String],
                     utils: PluginUtils): DataFrame => DataFrame =
    df => {
      val model = MAPE(labelCol, predictionCol, df, keywords, searchId)
      model.makeEvaluate()
    }
}
