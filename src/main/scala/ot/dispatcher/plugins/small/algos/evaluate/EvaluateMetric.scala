package ot.dispatcher.plugins.small.evaluate

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

trait EvaluateMetric{
  def makeEvaluate(): DataFrame
}