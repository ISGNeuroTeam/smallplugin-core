package ot.dispatcher.plugins.small.evaluate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.stat.Summarizer

/**
 * Evaluator for regression
 * @param labelCol - observation column
 * @param predictionCol - prediction column
 * @param dataFrame - dataFrame with two double columns: prediction and observation
 * @param metricName - name of the evaluated metric
 * @param featuresNumber - number of features in the model
 * @param searchId - search ID
 */
case class RegressionScore(labelCol: String, predictionCol: List[String], dataFrame: DataFrame, metricName: String, featuresNumber: Double, searchId: Int) extends EvaluateMetric {

  /**
   * Performs metric calculation
   *
   * @return dataframe with one column: metricName with metric value
   */
  def makeEvaluate(): DataFrame = {
    val resultDF = metricName match {
      case "mse" => calcMse()
      case "rmse" => calcRmse()
      case "mae" => calcMae()
      case "mape" => calcMape()
      case "smape" => calcSmape()
      case "r2" => calcR2()
      case "r2_adj" => calcAdjR2()
    }
    resultDF
  }

  /**
   * Return the mean squared error
   */
  def calcMse(): DataFrame = {
    val resultDf = dataFrame.withColumn("error", pow(dataFrame(labelCol) - dataFrame(predictionCol.head), 2))
      .select("error")
      .agg(avg("error") as metricName)
    resultDf
  }

  /**
   * Returns the root mean squared error
   *
   */
  def calcRmse(): DataFrame = {
    val resultDf = dataFrame.withColumn("error", pow(dataFrame(labelCol) - dataFrame(predictionCol.head), 2))
      .select("error")
      .agg(sqrt(avg("error")) as metricName)
    resultDf
  }

  /**
   * Returns the mean absolute error
   *
   */
  def calcMae(): DataFrame = {
    val resultDF = dataFrame.withColumn("error", abs(dataFrame(labelCol) - dataFrame(predictionCol.head)))
      .select("error")
      .agg(avg("error") as metricName)
    resultDF
  }

  /**
   * Returns the mean absolute percentage error
   *
   */
  def calcMape(): DataFrame = {
    val resultDF = dataFrame
      .withColumn("error", abs((dataFrame(labelCol) - dataFrame(predictionCol.head)) / dataFrame(labelCol)))
      .select("error")
      .agg(avg("error") as metricName)
    resultDF
  }

  /**
   * Returns the symmetric mean absolute percentage error
   * @return
   */
  def calcSmape(): DataFrame = {

    val numenatorDenominatorDf = dataFrame
      .withColumn("numenator", abs(dataFrame(labelCol) - dataFrame(predictionCol.head)))
      .withColumn("denominator", ((abs(dataFrame(labelCol)) + abs(dataFrame(predictionCol.head))) / 2))
    val meanFractDf = numenatorDenominatorDf
      .withColumn("fract", numenatorDenominatorDf("numenator") / numenatorDenominatorDf("denominator"))
      .select("fract")
      .agg(avg("fract") as "meanFract")
    val resultDf = meanFractDf
      .withColumn(metricName, meanFractDf("meanFract") * lit(100))
      .select(metricName)
    resultDf
  }

  /**
   * Returns the coefficient of determination (R2)
   *
   */
  def calcR2(): DataFrame = {
    val meanDf = dataFrame
      .select(labelCol)
      .agg(avg(labelCol) as "meanLabelCol")
    val meanFullDf = dataFrame.crossJoin(meanDf)
    val dfWithSS = meanFullDf
      .withColumn("squareResidual", pow(meanFullDf(labelCol) - meanFullDf(predictionCol.head), 2))
      .withColumn("squareTotal", pow(meanFullDf(labelCol) - meanFullDf("meanLabelCol"), 2))
      .select("squareTotal", "squareResidual")
      .agg(sum("squareResidual") as "sumSquareResidual", sum( "squareTotal") as "sumSquareTotal")
    val resultDf = dfWithSS
      .withColumn(metricName, lit(1) - dfWithSS("sumSquareResidual") / dfWithSS("sumSquareTotal"))
      .select(metricName)
    resultDf
  }

  /**
   *  Returns the adjusted coefficient of determination (R2_adj)
   *
   */
  def calcAdjR2(): DataFrame = {
    val lengthDf = dataFrame.count()
    val coefDetermination = calcR2()
    val resultDf = coefDetermination
      .withColumn(metricName, lit(1) - (lit(1) - coefDetermination("r2_adj")) * lit(lengthDf - 1) / lit(lengthDf - featuresNumber))
      .select(metricName)
    resultDf
  }

}
