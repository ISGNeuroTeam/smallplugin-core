package ot.dispatcher.plugins.small.algos.fit

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.small.sdk.FitModel
import ot.dispatcher.sdk.PluginUtils

case class Classification (featureCols: List[String], targetCol: String, dataFrame: DataFrame, modelName: String, searchId:Int) {

  import org.apache.spark.ml.classification.LogisticRegression

  def createPipeline(): Pipeline = {
    val featuresName = s"__${modelName}_features__"
    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol(featuresName)
    val indexedLabel = s"__indexed${targetCol}__"

    val labelIndexer = new StringIndexer()
      .setInputCol(targetCol)
      .setOutputCol(indexedLabel)
      .fit(dataFrame)

    val lr = new LogisticRegression()
      .setLabelCol(indexedLabel)
      .setFeaturesCol(featuresName)
      .setPredictionCol(modelName + "_index_prediction")
    val labelConverter = new IndexToString()
      .setInputCol(modelName + "_index_prediction")
      .setOutputCol(modelName + "_prediction")
      .setLabels(labelIndexer.labels)
    new Pipeline().setStages(Array(assembler, labelIndexer, lr, labelConverter))
  }

  def prepareDf() = {
    dataFrame.drop(modelName + "_prediction").filter(targetCol + " is not null")
  }

  def makePrediction(): Tuple2[PipelineModel, DataFrame] = {
    val p = createPipeline()
    val df = prepareDf()
    val pModel = p.fit(df)
    val rdf = pModel.transform(df).drop(s"__${modelName}_features__")
    (pModel, rdf)
  }
}

object Classification extends FitModel {
  override def fit(modelName: String, searchId: Int, featureCols: List[String], targetCol: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => (PipelineModel, DataFrame) =
    df => {
      val model = targetCol
        .map(Classification(featureCols, _, df, modelName, searchId))
        .getOrElse(
          utils.sendError(searchId, "Target column name is not provided.")
        )
      model.makePrediction()
    }
}

