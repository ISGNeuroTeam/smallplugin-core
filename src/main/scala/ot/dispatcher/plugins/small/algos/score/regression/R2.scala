package ot.dispatcher.plugins.small.algos.score.regression

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.ScoreModel
import ot.dispatcher.sdk.PluginUtils

/**
 * Evaluator for R2 metric
 * @param labelCol - observation column
 * @param predictionCol - prediction column
 * @param df - dataFrame with two double columns: prediction and observation
 * @param keywords - pairs of key-value
 * @param searchId - search ID
 */
case class R2(labelCol: String, predictionCol: String, df: DataFrame, keywords: Map[String, String], searchId: Int) {

  /**
   * Performs R2 metric calculation
   *
   * @return dataframe with one column: metricName with metric value
   */
  def makeEvaluate(): DataFrame = {
    val meanDf = df
      .select(labelCol)
      .agg(avg(labelCol) as "meanLabelCol")
    val meanFullDf = df.crossJoin(meanDf)
    val dfWithSS = meanFullDf
      .withColumn("squareResidual", pow(meanFullDf(labelCol) - meanFullDf(predictionCol), 2))
      .withColumn("squareTotal", pow(meanFullDf(labelCol) - meanFullDf("meanLabelCol"), 2))
      .select("squareTotal", "squareResidual")
      .agg(sum("squareResidual") as "sumSquareResidual", sum( "squareTotal") as "sumSquareTotal")
    dfWithSS
      .withColumn("R2", lit(1) - dfWithSS("sumSquareResidual") / dfWithSS("sumSquareTotal"))
      .select("R2")
  }
}

object R2 extends ScoreModel {
  override def score(modelName: String,
                     modelConfig: Option[Config],
                     searchId: Int,
                     labelCol: String,
                     predictionCol: String,
                     keywords: Map[String, String],
                     utils: PluginUtils): DataFrame => DataFrame =
    df => {
      val model = R2(labelCol, predictionCol, df, keywords, searchId)
      model.makeEvaluate()
    }
}
