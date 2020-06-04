package ot.dispatcher.plugins.small.algos.apply

import org.apache.spark.sql.DataFrame

trait ApplyAlgorithm{
  def makePrediction(df: DataFrame): DataFrame
}

