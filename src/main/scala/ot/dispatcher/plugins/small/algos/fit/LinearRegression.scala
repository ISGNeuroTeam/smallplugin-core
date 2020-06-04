package ot.dispatcher.plugins.small.algos.fit

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

case class LinearRegression(featureCols: List[String], targetCol: String, dataFrame: DataFrame, modelName: String, searchId:Int) extends FitAlgorithm {
  def createPipeline(): Pipeline = {
    val featuresName = s"__${modelName}_features__"
    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol(featuresName)
    val lr = new org.apache.spark.ml.regression.LinearRegression()
      .setLabelCol(targetCol)
      .setFeaturesCol(featuresName)
      .setPredictionCol(modelName + "_prediction")
    new Pipeline().setStages(Array(assembler, lr))
  }

  def prepareDf() ={
    dataFrame.drop(modelName + "_prediction").filter(targetCol +  " is not null")
  }

  def makePrediction(): Tuple2[PipelineModel, DataFrame] = {
    val p = createPipeline()
    val df = prepareDf()
    val pModel = p.fit(df)
    val rdf = pModel.transform(df).drop(s"__${modelName}_features__")
    (pModel, rdf)
  }
}


