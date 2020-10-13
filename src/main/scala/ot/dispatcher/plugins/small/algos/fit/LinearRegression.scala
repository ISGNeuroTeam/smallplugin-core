package ot.dispatcher.plugins.small.algos.fit

import com.typesafe.config.Config
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.FitModel
import ot.dispatcher.sdk.PluginUtils

case class LinearRegression(featureCols: List[String], targetCol: String, dataFrame: DataFrame, modelName: String, searchId:Int) {
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

object LinearRegression extends FitModel {
  override def fit(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String], targetCol: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => (PipelineModel, DataFrame) =
    df => {
      targetCol
        .map(LinearRegression(featureCols, _, df, modelName, searchId))
        .map(_.makePrediction())
        .getOrElse(
          utils.sendError(searchId, "Target column name is not provided.")
        )
    }
}


