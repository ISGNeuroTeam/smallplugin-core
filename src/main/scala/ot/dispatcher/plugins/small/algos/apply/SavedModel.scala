package ot.dispatcher.plugins.small.algos.apply

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import ot.dispatcher.plugins.small.utils.SmallModelsUtils
import ot.dispatcher.sdk.PluginUtils

import scala.util.{Failure, Success, Try}

case class SavedModel(modelName: String, id: Int, utils: PluginUtils) extends SmallModelsUtils(utils) {
  import utils._
  def makePrediction(df: DataFrame): DataFrame = {
    val model = Try(loadExisting(modelName, id)) match {
      case Success(x) => x
      case Failure(e) => sendError(id, s"Model with name '$modelName' is not found")
    }
    val mdf = df.drop(df.columns.filter(_.matches(".*_prediction")).toArray : _*)
    val tdf = model.transform(mdf)
    val serviceCols =  tdf.columns.filter(_.matches("__.*__"))
    val fdf = tdf.drop(serviceCols : _*)
    val lastPipeline= model.stages.last
    lastPipeline.extractParamMap().toSeq.find(_.param.name == "predictionCol") match {
      case Some(r) => {
        val lastRes = r.value.toString //Get column name of last operation in pipeline
        fdf.withColumn(modelName + "_prediction", col(lastRes))
      }
      case None => fdf
    }
  }
}
