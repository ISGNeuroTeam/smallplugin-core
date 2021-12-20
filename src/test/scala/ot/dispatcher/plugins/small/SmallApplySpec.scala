package ot.dispatcher.plugins.small

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.reflect.io.Path
import scala.util.{Failure, Random, Success, Try}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.FixtureAnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Outcome}
import ot.dispatcher.plugins.small.commands.SmallApply
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.{CommandTest, CommandTestException, MockPluginUtils}


class SmallApplySpec extends FixtureAnyFlatSpec with BeforeAndAfterAll with Matchers {

  import SmallApplySpec._

  case class FixtureParam(parameters: Future[ApplyParameters])

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  private lazy val utils: PluginUtils =
    new MockPluginUtils(sparkSession)

  private def deleteConfiguredDirectory(config: Config)(section: String): Unit =
    Try(config.getString(section)).map(Path(_)) match {
      case Failure(exception) =>
        fail(exception.getMessage)
      case Success(path) if path.exists && path.isDirectory =>
        path.deleteRecursively shouldBe true
      case Success(path) if path.exists =>
        fail(s"The path '$path' defined at the config section '$section' is not a directory.")
      case _ =>
        ()
    }

  private def deleteMemCacheDir(): Unit =
    deleteConfiguredDirectory(utils.mainConfig)("memcache.path")

  private def deletePersistentDir(): Unit =
    deleteConfiguredDirectory(utils.pluginConfig)("mlmodels.path")


  override def beforeAll(): Unit = {
    deleteMemCacheDir()
    deletePersistentDir()
  }

  override def afterAll(): Unit = {
    deleteMemCacheDir()
    deletePersistentDir()
  }

  override protected def withFixture(test: OneArgTest): Outcome = {
    val theFixture: FixtureParam =
      FixtureParam(parametersToComplete.future)

    try {
      withFixture(test.toNoArgTest(theFixture))
    }
    finally {
      ()
    }
  }

  private implicit class QueryRunner(query: SimpleQuery) {
    def run(df: DataFrame): DataFrame =
      new SmallApply(query, utils)
        .transform(df)
  }

  private val data: DataFrame =
    sparkSession.emptyDataFrame

  implicit val ec: ExecutionContext = ExecutionContext.global


  "The apply command" should "invoke a model implementation as defined in the `plugin.conf`." in { f =>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(model)

    query.run(data)

    f.parameters.isCompleted shouldBe true
  }

  it should "fail if a defined model implementation is not available." in { f =>
    val model: String = "unavailable"
    val query: SimpleQuery = SimpleQuery(model)

    an[CommandTestException] should be thrownBy query.run(data)

    f.parameters.isCompleted shouldBe false
  }


  it should "pass to a model a specific configuration as defined in the `plugin.conf`." in { f =>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(model)

    query.run(data)

    val configBasePath: Path = Path(utils.pluginConfig.getString("configBasePath"))
      .resolve("conf/apply.conf")

    val expected: Config = ConfigFactory
      .parseURL(configBasePath.toURL)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelConfig shouldBe Some(expected)
  }


  it should "fail if a model's specific configuration defined in the `plugin.conf` is failed to load." in { f =>
    val model: String = "misconfigured"
    val query: SimpleQuery = SimpleQuery(model)

    an[CommandTestException] should be thrownBy query.run(data)

    f.parameters.isCompleted shouldBe false
  }


  it should "pass no specific configuration if it is not defined in the `plugin.conf`." in { f =>
    val model: String = "unconfigured"
    val query: SimpleQuery = SimpleQuery(model)

    query.run(data)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelConfig shouldBe None
  }


  it should "pass the name of target column to a model implementation." in { f =>
    val model: String = "dummy"
    val targetName: Option[String] = Some("secretCol")
    val query: SimpleQuery = SimpleQuery(s"$model ${targetName.get}")
    query.run(data)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.targetName shouldBe targetName
  }


  it should "pass no name of the target column to a model implementation if the target column is not provided." in { f =>
    val model: String = "dummy"
    val targetName: Option[String] = None
    val query: SimpleQuery = SimpleQuery(s"$model ${targetName.getOrElse("")}")
    query.run(data)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.targetName shouldBe targetName
  }


  it should "pass the names of feature columns to a model implementation." in { f =>
    val model: String = "dummy"
    val featureCols: List[String] = List("a", "b", "c")
    val query: SimpleQuery = SimpleQuery(s"$model target from ${featureCols.mkString(" ")}")
    query.run(data)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.featureCols shouldBe featureCols
  }


  it should "pass the plugin utilities to a model implementation." in { f =>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(model)
    query.run(data)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.utils shouldBe utils
  }


  it should "pass the keywords to a model implementation." in { f =>
    val model: String = "dummy"
    val keywords: Map[String, String] = Map(
      "taram" -> "param",
      "magic" -> "2020-10-20"
    )

    val keywordsString = keywords
      .map { case (k, v) => k + "=" + v }
      .mkString(" ")

    val query: SimpleQuery = SimpleQuery(s"$model $keywordsString")
    query.run(data)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.keywords shouldBe keywords
  }

  it should "pass the search identifier to a model implementation." in { f =>
    val model: String = "dummy"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(model, searchId)
    query.run(data)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.searchId shouldBe query.searchId
  }


  it should "fail if the provided model name is unknown and the model can not be loaded from the cache or persistent storage." in { f =>
    val model: String = "unsaved"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(model, searchId)

    val modelCachePath: Path =
      Path(utils.mainConfig.getString("memcache.path"))
        .resolve(s"search_${query.searchId}.cache")
        .resolve(model)

    val modelPersistentPath: Path =
      Path(utils.pluginConfig.getString("mlmodels.path"))
        .resolve(model)

    modelCachePath.exists shouldBe false
    modelPersistentPath.exists shouldBe false

    an[CommandTestException] should be thrownBy query.run(data)
  }


  private def saveModel(model: PipelineModel, path: String): Boolean =
    Try(model.save(path)).isSuccess

  private val dummyModel: PipelineModel = {
    val stage = new SQLTransformer("dummy")
      .setStatement("SELECT * FROM __THIS__")

    val pipeline = new Pipeline("dummy")
      .setStages(Array(stage))

    pipeline.fit(sparkSession.emptyDataFrame)
  }

  it should "try to use the saved model from the cache storage if the provided model name is unknown." in { f =>
    val model: String = "cached"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(model, searchId)

    val modelCachePath: Path =
      Path(utils.mainConfig.getString("memcache.path"))
        .resolve(s"search_${query.searchId}.cache")
        .resolve(model)

    val modelPersistentPath: Path =
      Path(utils.pluginConfig.getString("mlmodels.path"))
        .resolve(model)

    modelCachePath.exists shouldBe false
    saveModel(dummyModel, modelCachePath.path) shouldBe true

    modelPersistentPath.exists shouldBe false

    modelCachePath.exists shouldBe true
    modelCachePath.isDirectory shouldBe true

    query.run(data)

    f.parameters.isCompleted shouldBe false
  }


  it should "try to use the saved model from the persistent storage if the provided model name is unknown and is model not cache storage." in { f =>
    val model: String = "persistent"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(model, searchId)

    val modelCachePath: Path =
      Path(utils.mainConfig.getString("memcache.path"))
        .resolve(s"search_${query.searchId}.cache")
        .resolve(model)

    val modelPersistentPath: Path =
      Path(utils.pluginConfig.getString("mlmodels.path"))
        .resolve(model)

    modelPersistentPath.exists shouldBe false
    saveModel(dummyModel, modelPersistentPath.path) shouldBe true

    modelCachePath.exists shouldBe false

    modelPersistentPath.exists shouldBe true
    modelPersistentPath.isDirectory shouldBe true

    query.run(data)

    f.parameters.isCompleted shouldBe false
  }


  it should "drop a columns that starts and ends with double underscore characters from the resulting data frame." in { f =>
    import sparkSession.implicits._

    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(model)
    val source: DataFrame = Seq((1, 2, 3, 4, 5)).toDF("a", "b", "c", "__d__", "__e__")
    val result: DataFrame = query.run(source)

    f.parameters.isCompleted shouldBe true

    val predicate: String => Boolean =
      s => s.startsWith("__") && s.endsWith("__")

    source.columns.exists(predicate) shouldBe true
    result.columns.exists(predicate) shouldBe false
  }


  it should "drop a columns that ends with `_prediction` before data frame passed to cached model." in { f =>
    import sparkSession.implicits._

    val model: String = "cached"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(model, searchId)

    val modelCachePath: Path =
      Path(utils.mainConfig.getString("memcache.path"))
        .resolve(s"search_${query.searchId}.cache")
        .resolve(model)

    val modelPersistentPath: Path =
      Path(utils.pluginConfig.getString("mlmodels.path"))
        .resolve(model)

    modelCachePath.exists shouldBe false
    saveModel(dummyModel, modelCachePath.path) shouldBe true

    modelPersistentPath.exists shouldBe false

    modelCachePath.exists shouldBe true
    modelCachePath.isDirectory shouldBe true


    val source: DataFrame = Seq((1, 2, 3, 4, 5)).toDF("a", "b", "c", "d_prediction", "e_prediction")
    val result: DataFrame = query.run(source)

    f.parameters.isCompleted shouldBe false

    val predicate: String => Boolean =
      s => s.endsWith("_prediction")

    source.columns.exists(predicate) shouldBe true
    result.columns.exists(predicate) shouldBe false
  }


  it should "drop a columns that ends with `_prediction` before data frame passed to persistent model." in { f =>
    import sparkSession.implicits._

    val model: String = "drop_prediction"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(model, searchId)

    val modelCachePath: Path =
      Path(utils.mainConfig.getString("memcache.path"))
        .resolve(s"search_${query.searchId}.cache")
        .resolve(model)

    val modelPersistentPath: Path =
      Path(utils.pluginConfig.getString("mlmodels.path"))
        .resolve(model)

    modelPersistentPath.exists shouldBe false
    saveModel(dummyModel, modelPersistentPath.path) shouldBe true

    modelCachePath.exists shouldBe false

    modelPersistentPath.exists shouldBe true
    modelPersistentPath.isDirectory shouldBe true


    val source: DataFrame = Seq((1, 2, 3, 4, 5)).toDF("a", "b", "c", "d_prediction", "e_prediction")
    val result: DataFrame = query.run(source)

    f.parameters.isCompleted shouldBe false

    val predicate: String => Boolean =
      s => s.endsWith("_prediction")

    source.columns.exists(predicate) shouldBe true
    result.columns.exists(predicate) shouldBe false
  }

  it should "rename a column defined in the saved model's `predictionCol` parameter to the `modelName` that ends with `_prediction` before data frame returned." in { f =>
    cancel("Implementation is too complex and time costly to be done at this time.")
  }

  it should "work with missing data" in { _ =>
    val model: String = "dummy"
    val featureCols: List[String] = List("a", "b", "c")
    val query: SimpleQuery = SimpleQuery(s"$model target from ${featureCols.mkString(" ")}")

    val inputDataset: String ="""[
                                |{"_time":null, "strtime":"","a":1, "b":2, "c":2, "target":"1a", "class":"1a" },
                                |{"_time":1,"strtime":"e","a":7, "b":6, "c":2, "target":"2b", "class":"2b" },
                                |{"_time":1,"strtime":"f","a":7, "b":null, "c":2, "target":"2b", "class":"2b" },
                                |{"_time":1,"strtime":"x","a":7, "b":8, "c":2, "target":"", "class":"" }
                                | ]""".stripMargin
    val inputWithMissing = new CommandTest {override val dataset: String = inputDataset}.jsonToDf(inputDataset)

    val actual = query.run(inputWithMissing)

    actual.count() shouldBe 3.toLong
  }
}

object SmallApplySpec {

  case class ApplyParameters(
    modelName: String,
    modelConfig: Option[Config],
    searchId: Int,
    featureCols: List[String],
    targetName: Option[String],
    keywords: Map[String, String],
    utils: PluginUtils
  )

  private val parameters: Stream[Promise[ApplyParameters]] =
    Stream.continually(Promise())

  def parametersToComplete: Promise[ApplyParameters] =
    parameters.dropWhile(_.isCompleted).head

}