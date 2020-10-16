package ot.dispatcher.plugins.small.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.utils.SmallModelsUtils
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._


class SmallSave(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq,utils){
  val smallUtils = new SmallModelsUtils(utils)
  import smallUtils._
  val wordsFromQuery = getFieldsUsed(returns)
  override val fieldsUsed = List()
  val finalName = wordsFromQuery(wordsFromQuery.indexOf("`as`") + 1).stripBackticks()

  def transform(_df: DataFrame): DataFrame = {
    val pipelineNames= wordsFromQuery.takeWhile(_ != "`as`").map(_.stripBackticks())
    val fdf = _df.drop(pipelineNames.map(_ + "_prediction").toArray : _*)
    val finalPipeline = merge(fdf, pipelineNames, finalName, sq.searchId)
    val tdf = finalPipeline.transform(fdf)
    val serviceCols =  tdf.columns.filter(_.matches("__.*__"))
    val res= tdf.drop(serviceCols: _*)
    res.show
    res
  }
}
