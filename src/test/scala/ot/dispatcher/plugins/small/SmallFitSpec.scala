package ot.dispatcher.plugins.small

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.reflect.io.Path
import scala.util.{Failure, Random, Success, Try}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, Outcome, fixture}
import ot.dispatcher.plugins.small.commands.SmallFit
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.{CommandTest, CommandTestException, MockPluginUtils}


class SmallFitSpec extends fixture.FlatSpec with BeforeAndAfterAll with Matchers {

  import SmallFitSpec._

  case class FixtureParam(parameters: Future[FitParameters])

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  private lazy implicit val utils: PluginUtils =
    new MockPluginUtils(sparkSession)

  private def alterPluginConfig(patch: Config => Config): PluginUtils => PluginUtils =
    pu => new PluginUtils {
      override def getLoggerFor(classname: String): Logger =
        pu.getLoggerFor(classname)

      override def logLevelOf(name: String): String =
        pu.logLevelOf(name)

      override def printDfHeadToLog(log: Logger, id: Int, df: DataFrame): Unit =
        pu.printDfHeadToLog(log, id, df)

      override def sendError(id: Int, message: String): Nothing =
        pu.sendError(id, message)

      override def spark: SparkSession =
        pu.spark

      override def executeQuery(query: String, df: DataFrame): DataFrame =
        pu.executeQuery(query, df)

      override def executeQuery(query: String, index: String, startTime: Int, finishTime: Int): DataFrame =
        pu.executeQuery(query, index, startTime, finishTime)

      private val patchedPluginConfig: Config =
        patch(pu.pluginConfig)

      override def pluginConfig: Config =
        patchedPluginConfig

      override def mainConfig: Config =
        pu.mainConfig
    }

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

  override def beforeAll(): Unit =
    deleteMemCacheDir()

  override def afterAll(): Unit = {
    deleteMemCacheDir()
    sparkSession.close()
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

  private val train: DataFrame =
    sparkSession.emptyDataFrame

  implicit class QueryRunner(query: SimpleQuery) {
    def run(df: DataFrame)(implicit utils: PluginUtils): DataFrame =
      new SmallFit(query, utils)
        .transform(df)
  }

  implicit val ec: ExecutionContext = ExecutionContext.global


  "The fit command" should "invoke a model implementation as defined in the `plugin.conf`." in { f =>
    val model: String = "dummy"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c", searchId)

    query.run(train)

    f.parameters.isCompleted shouldBe true

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelName shouldBe s"$model-${query.searchId}"
  }


  it should "fail if a model implementation not defined in `plugin.conf`." in { f =>
    val model: String = "none"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c")

    an [CommandTestException] should be thrownBy query.run(train)

    f.parameters.isCompleted shouldBe false
  }


  it should "fail if a defined model implementation is not available." in { f =>
    val model: String = "unavailable"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c")

    an [CommandTestException] should be thrownBy query.run(train)

    f.parameters.isCompleted shouldBe false
  }


  it should "pass to a model a specific configuration as defined in the `plugin.conf`." in { f =>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c")

    query.run(train)

    val configBasePath: Path = Path(utils.pluginConfig.getString("configBasePath"))
        .resolve("conf/fit.conf")

    val expected: Config = ConfigFactory
      .parseURL(configBasePath.toURL)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelConfig shouldBe Some(expected)
  }


  it should "fail if a model's specific configuration defined in the `plugin.conf` is failed to load." in { f =>
    val model: String = "misconfigured"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c")

    an [CommandTestException] should be thrownBy query.run(train)

    f.parameters.isCompleted shouldBe false
  }


  it should "pass no specific configuration if it is not defined in the `plugin.conf`." in { f =>
    val model: String = "unconfigured"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c")

    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelConfig shouldBe None
  }


  it should "cache a fitted model with the provided alias name in the temporary storage defined in the `application.conf`." in { f=>
    val model: String = "dummy"
    val alias: String = "surprise"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c into $alias")

    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelName shouldBe alias

    val modelPath: Path =
      Path(utils.mainConfig.getString("memcache.path"))
        .resolve(s"search_${parameters.searchId}.cache")
        .resolve(parameters.modelName)

    modelPath.exists shouldBe true
    modelPath.isDirectory shouldBe true
  }


  it should "cache a fitted model with the model name in the temporary storage defined in the `application.conf`." in { f=>
    val model: String = "dummy"
    val searchId: Int = scala.util.Random.nextInt()
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c", searchId)

    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.searchId shouldBe query.searchId

    val modelPath: Path =
      Path(utils.mainConfig.getString("memcache.path"))
        .resolve(s"search_${parameters.searchId}.cache")
        .resolve(parameters.modelName)

    modelPath.exists shouldBe true
    modelPath.isDirectory shouldBe true
  }


  it should "drop a columns that starts and ends with double underscore characters from the resulting data frame." in { f =>
    import sparkSession.implicits._

    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c")
    val source: DataFrame = Seq((1, 2, 3, 4, 5)).toDF("a", "b", "c", "__d__", "__e__")
    val result: DataFrame = query.run(source)

    f.parameters.isCompleted shouldBe true

    val predicate: String => Boolean =
      s => s.startsWith("__") && s.endsWith("__")

    source.columns.exists(predicate) shouldBe true
    result.columns.exists(predicate) shouldBe false
  }


  it should "pass the names of feature columns to a model implementation." in { f =>
    val model: String = "dummy"
    val featureCols: List[String] = List("a", "b", "c")
    val query: SimpleQuery = SimpleQuery(s"$model target from ${featureCols.mkString(" ")}")
    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.featureCols shouldBe featureCols
  }


  it should "pass the provided alias name as the model name to a model implementation." in { f =>
    val model: String = "dummy"
    val alias: String = "modelX"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c into $alias")
    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelName shouldBe alias
  }


  it should "pass the name of target column to a model implementation." in { f=>
    val model: String = "dummy"
    val targetCol: Option[String] = Some("secretCol")
    val query: SimpleQuery = SimpleQuery(s"$model ${targetCol.get} from a b c")
    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.targetCol shouldBe targetCol
  }


  it should "pass no name of the target column to a model implementation if the target column is not provided ." in { f=>
    val model: String = "dummy"
    val targetCol: Option[String] = None
    val query: SimpleQuery = SimpleQuery(s"$model ${targetCol.getOrElse("")} from a b c")
    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.targetCol shouldBe targetCol
  }


  it should "pass the search identifier to a model implementation." in { f=>
    val model: String = "dummy"
    val searchId: Int = scala.util.Random.nextInt()
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c", searchId)
    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.searchId shouldBe query.searchId
  }


  it should "pass the plugin utilities to a model implementation." in { f=>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c")
    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.utils shouldBe utils
  }


  it should "pass the keywords to a model implementation." in { f=>
    val model: String = "dummy"
    val keywords: Map[String, String] = Map(
      "taram" -> "param",
      "magic" -> "2020-10-20"
    )

    val keywordsString = keywords
      .map { case (k, v) => k + "=" + v }
      .mkString(" ")

    val query: SimpleQuery = SimpleQuery(s"$model $keywordsString from a b c")
    query.run(train)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.keywords shouldBe keywords
  }

  it should "invoke the default model implementation when the requested model is not defined in the config `plugin.conf`." in { f =>
    val model: String = "none"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c", searchId)

    val addDefault: Config => Config =
      cfg =>
        cfg.withValue(
          "fit.default",
          ConfigValueFactory.fromAnyRef(cfg.getString("fit.dummy"))
        )

    val alteredUtils: PluginUtils = alterPluginConfig(addDefault)(utils)

    query.run(train)(alteredUtils)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelName shouldBe s"$model-${query.searchId}"
  }

  it should "invoke the default model implementation with provided model's alias name when the requested model is not defined in the config `plugin.conf`." in { f =>
    val model: String = "none"
    val alias: String = "unbelievable"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c into $alias", searchId)

    val addDefault: Config => Config =
      cfg =>
        cfg.withValue(
          "fit.default",
          ConfigValueFactory.fromAnyRef(cfg.getString("fit.dummy"))
        )

    val alteredUtils: PluginUtils = alterPluginConfig(addDefault)(utils)

    query.run(train)(alteredUtils)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelName shouldBe alias
  }

  it should "work with missing data`." in { f =>
    val model: String = "dummy"
    val searchId: Int = Random.nextInt()
    val query: SimpleQuery = SimpleQuery(s"$model target from a b c", searchId)

    val inputDataset: String ="""[
                                        |{"_time":"","strtime":"","a":1, "b":2, "c":2, "target":"1a", "class":"1a" },
                                        |{"_time":1,"strtime":"e","a":7, "b":6, "c":2, "target":"2b", "class":"2b" },
                                        |{"_time":1,"strtime":"f","a":7, "b":"", "c":2, "target":"2b", "class":"2b" },
                                        |{"_time":1,"strtime":"x","a":7, "b":8, "c":2, "target":"", "class":"" }
                                        | ]""".stripMargin
    val inputWithMissing = new CommandTest {override val dataset: String = inputDataset}.jsonToDf(inputDataset)

    val actual = query.run(inputWithMissing)

    actual.count() shouldBe 2.toLong
  }

}


object SmallFitSpec {

  case class FitParameters(
    modelName: String,
    modelConfig: Option[Config],
    searchId: Int,
    featureCols: List[String],
    targetCol: Option[String],
    keywords: Map[String, String],
    utils: PluginUtils
  )

  private val parameters: Stream[Promise[FitParameters]] =
    Stream.continually(Promise())

  def parametersToComplete: Promise[FitParameters] =
    parameters.dropWhile(_.isCompleted).head

}