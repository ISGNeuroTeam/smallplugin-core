package ot.dispatcher.plugins.small.algos.fit

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

trait FitAlgorithm{
  def makePrediction(): Tuple2[PipelineModel, DataFrame]
}

