package ot.dispatcher.plugins.small.utils

import java.nio.file.Paths

import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

class SmallModelsUtils(pluginUtils: PluginUtils){
  import pluginUtils._
  val config = pluginConfig
  def merge(df: DataFrame, names: List[String], dstPath: String, searchId: Long) = {
    val allStages = names.foldLeft(List[Transformer]())((acc, name) => acc ++ loadExisting(name, searchId).stages).toArray
    val resPipeline = new Pipeline().setStages(allStages).fit(df)
    resPipeline.write.overwrite().save(getModelPath(dstPath))
    resPipeline
  }

  def save(pipeline: PipelineModel, name: String) = {
    pipeline.write.overwrite().save(getModelPath(name))
  }

  def toCache(pipeline: PipelineModel, name: String, searchId: Long) = {
    pipeline.write.overwrite().save(getTempModelPath(name, searchId))
  }

  def load(name: String): PipelineModel = {
    PipelineModel.load(getModelPath(name))
  }

  def fromCache(name: String, searchId: Long): PipelineModel = {
    PipelineModel.load(getTempModelPath(name, searchId))
  }

  private def getModelPath(file: String) = config.getString("mlmodels.fs") +
    "//" + Paths.get(config.getString("mlmodels.path")).toAbsolutePath.toString +
    "/" + file
  private def getTempModelPath(file: String, searchId: Long) = mainConfig.getString("memcache.fs") +
    "//" + Paths.get(mainConfig.getString("memcache.path")).toAbsolutePath.toString +
    "/" + s"search_$searchId.cache/" + file

  def loadExisting(name: String, searchId: Long) = Try(fromCache(name, searchId)) match {
    case Success(m) => m
    case Failure(_) => load(name)
  }

  def fixMissing(df: DataFrame, featureCols: List[String], targetCol: String): DataFrame = {

    val getConf = config.getString("missing")
    println(s"filling strategy: $getConf")
    val allCols = targetCol::featureCols

    val resultDf = getConf match {
      case "skip" => {
        df.na.replace(allCols,Map("" -> null)).na.drop(allCols)
      }
      case "fillnull" => {
        df.na.fill(0, allCols).na.replace(allCols,Map("" -> "none"))
      }
      case _ => df
    }
    resultDf
  }
}
