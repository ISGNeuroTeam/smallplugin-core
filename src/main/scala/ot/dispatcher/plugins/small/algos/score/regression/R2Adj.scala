package ot.dispatcher.plugins.small.algos.score.regression

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.ScoreModel
import ot.dispatcher.sdk.PluginUtils
import scala.util.{Try, Success, Failure}

/**
 * Evaluator for adjusted R2 metric
 * @param labelCol - observation column
 * @param predictionCol - prediction column
 * @param df - dataFrame with two double columns: prediction and observation
 * @param keywords - pairs of key-value
 * @param searchId - search ID
 */
case class R2Adj(labelCol: String, predictionCol: String, df: DataFrame, keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  val featuresNumber: Int = keywords.get("featuresNumber") match {
    case Some(num) => Try(num.toInt) match {
      case Success(intNum) => intNum
      case _ => utils.sendError(searchId, "Number of features must be integer")
    }
    case _ => utils.sendError(searchId,"Number of features is not specified")
  }

  /**
   * Performs adjusted R2 metric calculation
   *
   * @return dataframe with one column: metricName with metric value
   */
  def makeEvaluate(): DataFrame = {
    println(featuresNumber)
    println(keywords)
    val lengthDf = df.count()

    val r2df = R2(labelCol, predictionCol, df, keywords, searchId).makeEvaluate()
    r2df.withColumn("R2Adj", lit(1) - (lit(1) - r2df("r2")) * lit(lengthDf - 1) / lit(lengthDf - featuresNumber))
      .select("R2Adj")
  }
}

object R2Adj extends ScoreModel {
  override def score(modelName: String,
                     modelConfig: Option[Config],
                     searchId: Int,
                     labelCol: String,
                     predictionCol: String,
                     keywords: Map[String, String],
                     utils: PluginUtils): DataFrame => DataFrame = {
    df => {
      val model = R2Adj(labelCol, predictionCol, df, keywords, searchId, utils)
      model.makeEvaluate()
    }
  }
}
