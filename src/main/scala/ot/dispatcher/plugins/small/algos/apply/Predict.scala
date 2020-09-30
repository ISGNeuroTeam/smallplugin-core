package ot.dispatcher.plugins.small.algos.apply

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.max
import ot.dispatcher.plugins.small.algos.fit.LinearRegression
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.extensions.DataFrameExt._
import ot.dispatcher.sdk.core.functions.Datetime

import scala.util.{Failure, Success, Try}

case class Predict(targetCol: String, keywords: Map[String, String], id: Int, utils: PluginUtils) extends ApplyAlgorithm{
  import utils._
  val log = getLoggerFor(this.getClass.getName())
  override def makePrediction(df: DataFrame): DataFrame = {
    val DefaultNum = 100
    val future = keywords.get("future") match {
      case Some(x) => Try(x.toInt) match {
        case Success(i) => i
        case Failure(_) => sendError(id, "The value of parameter 'future' should be of number type")
      }
      case None => DefaultNum
    }

    val span = Datetime.getSpanInSeconds(keywords.getOrElse("span","1d"))

    val maxTime = df.agg(max("_time")).collect()(0).getLong(0)
    //log.debug("[SearchId:${id}]\n maxTime=" + maxTime)
    val model = LinearRegression(List("_time"), targetCol, df, "temp", id)
    val (pModel,pDf) = model.makePrediction()
    val timeDf = spark.range(maxTime + span, maxTime + span * (future + 1), span).withColumnRenamed("id", "_time")
    printDfHeadToLog(log, id, timeDf)
    val predictedDf = pModel.transform(timeDf)
    printDfHeadToLog(log, id, predictedDf)
    val res = pDf.append(predictedDf)
    val serviceCols =  res.columns.filter(_.matches("__.*__"))
    val res1 = res.drop(serviceCols : _*).withSafeColumnRenamed("temp_prediction", "prediction")
    printDfHeadToLog(log, id, res1)
    res1
  }
}

object Predict extends ApplyModel {
  override def apply(searchId: Int, featureCols: List[String], targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame =>DataFrame = {
    val predictModel = targetName
      .map(Predict(_, keywords, searchId, utils))
      .getOrElse(utils.sendError(searchId, "Value column name is not specified"))
    predictModel.makePrediction
  }
}
