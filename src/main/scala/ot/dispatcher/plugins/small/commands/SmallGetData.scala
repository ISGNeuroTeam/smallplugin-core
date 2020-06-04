package ot.dispatcher.plugins.small.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

class SmallGetData(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
  import utils._
  override def transform(_df: DataFrame): DataFrame = {

    val format = new java.text.SimpleDateFormat("yyyy/MM/dd:HH:mm:ss")

    val indexName = """object=([a-zA-Z0-9_*-]+)""".r.findFirstMatchIn(sq.args) match {
      case Some(x) => x.group(1)
      case None => sendError(s"Object name is not defined")
    }

    val tws = """earliest=([0-9]+)""".r.findFirstMatchIn(sq.args) match {
      case Some(x) => x.group(1).toInt
      case None => 0
    }
    val twf = """latest=([0-9]+)""".r.findFirstMatchIn(sq.args) match {
      case Some(x) => x.group(1).toInt
      case None => 0
    }
    executeQuery("", indexName, tws, twf)
  }
}

