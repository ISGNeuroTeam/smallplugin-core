package ot.dispatcher.plugins.small.utils

import java.nio.file.Paths

import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils

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

  def fixMissing(df: DataFrame, cols: List[String]): DataFrame = {
    val getConf = config.getString("missing")
    val resultDf = getConf match {
      case "skip" =>
        if (df.isEmpty) df
        else df.na.replace(cols,Map("" -> null)).na.drop(cols)

      case "fillnull" =>
        val colsSet = cols.toSet
        val typeMap = df.dtypes.map( column =>
          column._2 match {
                case "StringType" => column._1 -> "none"
                case "DoubleType" => column._1 -> 0.0
                case "LongType" => column._1 -> 0.toLong
                case "BooleanType" => column._1 -> false
          }).toMap
        val colsMap = typeMap.filterKeys(colsSet.contains)
        df.na.replace(cols,Map("" -> "none")).na.fill(colsMap)

      case _ => df
    }
    resultDf
  }
}
