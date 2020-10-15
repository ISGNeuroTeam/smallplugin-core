package ot.dispatcher.plugins.small.algos.score.regression

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.ScoreModel
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.Keyword

/**
 * Evaluator for RMSE metric
 * @param labelCol - observation column
 * @param predictionCol - prediction column
 * @param df - dataFrame with two double columns: prediction and observation
 * @param keywords - pairs of key-value
 * @param searchId - search ID
 */
case class RMSE(labelCol: String, predictionCol: String, df: DataFrame, keywords: Map[String, String], searchId: Int) {

  /**
   * Performs MSE metric calculation
   *
   * @return dataframe with one column: metricName with metric value
   */
  def makeEvaluate(): DataFrame = {
    df.withColumn("error", pow(df(labelCol) - df(predictionCol), 2))
      .select("error")
      .agg(sqrt(avg("error")) as "RMSE")
  }
}

object RMSE extends ScoreModel {
  override def score(modelName: String,
                     modelConfig: Option[Config],
                     searchId: Int,
                     labelCol: String,
                     predictionCol: String,
                     keywords: Map[String, String],
                     utils: PluginUtils): DataFrame => DataFrame =
    df => {
      val model = RMSE(labelCol, predictionCol, df, keywords, searchId)
      model.makeEvaluate()
    }
}
