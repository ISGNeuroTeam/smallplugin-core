package ot.dispatcher.plugins.small.algos.score.regression

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.ScoreModel
import ot.dispatcher.sdk.PluginUtils

/**
 * Evaluator for SMAPE metric
 * @param labelCol - observation column
 * @param predictionCol - prediction column
 * @param df - dataFrame with two double columns: prediction and observation
 * @param keywords - pairs of key-value
 * @param searchId - search ID
 */
case class SMAPE(labelCol: String, predictionCol: String, df: DataFrame, keywords: Map[String, String], searchId: Int) {

  /**
   * Performs SMAPE metric calculation
   *
   * @return dataframe with one column: metricName with metric value
   */
  def makeEvaluate(): DataFrame = {
    val numenatorDenominatorDf = df
      .withColumn("numenator", abs(df(labelCol) - df(predictionCol)))
      .withColumn("denominator", ((abs(df(labelCol)) + abs(df(predictionCol))) / 2))
    val meanFractDf = numenatorDenominatorDf
      .withColumn("fract", numenatorDenominatorDf("numenator") / numenatorDenominatorDf("denominator"))
      .select("fract")
      .agg(avg("fract") as "meanFract")
    meanFractDf
      .withColumn("SMAPE", meanFractDf("meanFract") * lit(100))
      .select("SMAPE")
  }
}

object SMAPE extends ScoreModel {
  override def score(modelName: String,
                     modelConfig: Option[Config],
                     searchId: Int,
                     labelCol: String,
                     predictionCol: String,
                     keywords: Map[String, String],
                     utils: PluginUtils): DataFrame => DataFrame =
    df => {
      val model = SMAPE(labelCol, predictionCol, df, keywords, searchId)
      model.makeEvaluate()
    }
}
