package ot.dispatcher.plugins.small.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

class SmallGetData(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {

  override def transform(_df: DataFrame): DataFrame = {

    val indexName = """object=([a-zA-Z0-9_*-]+)""".r
      .findFirstMatchIn(sq.args)
      .map(_.group(1))
      .getOrElse(sendError(s"Object name is not defined"))

    val tws = """earliest=([0-9]+)""".r
      .findFirstMatchIn(sq.args)
      .map(_.group(1).toInt)
      .getOrElse(0)

    val twf = """latest=([0-9]+)""".r
      .findFirstMatchIn(sq.args)
      .map(_.group(1).toInt)
      .getOrElse(0)

    utils.executeQuery("", indexName, tws, twf)
  }
}

