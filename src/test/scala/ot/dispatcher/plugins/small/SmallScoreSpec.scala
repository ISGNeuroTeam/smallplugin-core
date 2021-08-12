package ot.dispatcher.plugins.small


import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.reflect.io.Path
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, Outcome, fixture}
import ot.dispatcher.plugins.small.commands.SmallScore
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.{CommandTestException, MockPluginUtils}


class SmallScoreSpec  extends fixture.FlatSpec with BeforeAndAfterAll with Matchers {

  import SmallScoreSpec._

  case class FixtureParam(parameters: Future[ScoreParameters])

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  private implicit val utils: PluginUtils =
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

  override def beforeAll(): Unit = ()

  override def afterAll(): Unit =
    sparkSession.close()

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

  private val evaluate: DataFrame =
    sparkSession.emptyDataFrame

  implicit class QueryRunner(query: SimpleQuery) {
    def run(df: DataFrame)(implicit utils: PluginUtils): DataFrame =
      new SmallScore(query, utils)
        .transform(df)
  }

  implicit val ec: ExecutionContext = ExecutionContext.global


  "The score command" should "invoke a model implementation as defined in the `plugin.conf`." in { f =>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(s"$model a vs b")

    query.run(evaluate)

    f.parameters.isCompleted shouldBe true
  }


  it should "fail if a model implementation not defined in `plugin.conf`." in { f =>
    val model: String = "none"
    val query: SimpleQuery = SimpleQuery(s"$model a vs b")

    an [CommandTestException] should be thrownBy query.run(evaluate)

    f.parameters.isCompleted shouldBe false
  }


  it should "fail if a defined model implementation is not available." in { f =>
    val model: String = "unavailable"
    val query: SimpleQuery = SimpleQuery(s"$model a vs b")

    an [CommandTestException] should be thrownBy query.run(evaluate)

    f.parameters.isCompleted shouldBe false
  }


  it should "pass to a model a specific configuration as defined in the `plugin.conf`." in { f =>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(s"$model a vs b")

    query.run(evaluate)

    val configBasePath: Path = Path(utils.pluginConfig.getString("configBasePath"))
      .resolve("conf/score.conf")

    val expected: Config = ConfigFactory
      .parseURL(configBasePath.toURL)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelConfig shouldBe Some(expected)
  }


  it should "fail if a model's specific configuration defined in the `plugin.conf` is failed to load." in { f =>
    val model: String = "misconfigured"
    val query: SimpleQuery = SimpleQuery(s"$model a vs b")

    an [CommandTestException] should be thrownBy query.run(evaluate)

    f.parameters.isCompleted shouldBe false
  }


  it should "pass no specific configuration if it is not defined in the `plugin.conf`." in { f =>
    val model: String = "unconfigured"
    val query: SimpleQuery = SimpleQuery(s"$model a vs b")

    query.run(evaluate)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelConfig shouldBe None
  }


  it should "fail if the label column name is not specified." in { f =>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(s"$model vs b")

    an [CommandTestException] should be thrownBy query.run(evaluate)

    f.parameters.isCompleted shouldBe false
  }


  it should "fail if the prediction column name is not specified." in { f =>
    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(s"$model a vs")

    an [CommandTestException] should be thrownBy query.run(evaluate)

    f.parameters.isCompleted shouldBe false
  }


  it should "pass the search identifier to a model implementation." in { f=>
    val model: String = "dummy"
    val searchId: Int = scala.util.Random.nextInt()
    val query: SimpleQuery = SimpleQuery(s"$model a vs b", searchId)
    query.run(evaluate)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.searchId shouldBe query.searchId
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

    val query: SimpleQuery = SimpleQuery(s"$model a vs b $keywordsString")
    query.run(evaluate)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.keywords shouldBe keywords
  }


  it should "drop a columns that starts and ends with double underscore characters from the resulting data frame." in { f =>
    import sparkSession.implicits._

    val model: String = "dummy"
    val query: SimpleQuery = SimpleQuery(s"$model a vs b")
    val source: DataFrame = Seq((1, 2, 3, 4, 5)).toDF("a", "b", "c", "__d__", "__e__")
    val result: DataFrame = query.run(source)

    f.parameters.isCompleted shouldBe true

    val predicate: String => Boolean =
      s => s.startsWith("__") && s.endsWith("__")

    source.columns.exists(predicate) shouldBe true
    result.columns.exists(predicate) shouldBe false
  }

  it should "invoke the default model implementation when the requested model is not defined in the config `plugin.conf`." in { f =>
    val model: String = "none"
    val query: SimpleQuery = SimpleQuery(s"$model a vs b")

    val addDefault: Config => Config =
      cfg =>
        cfg.withValue(
          "score.default",
          ConfigValueFactory.fromAnyRef(cfg.getString("score.dummy"))
        )

    val alteredUtils: PluginUtils = alterPluginConfig(addDefault)(utils)

    query.run(evaluate)(alteredUtils)

    val parameters = Await.result(f.parameters, 1 second)

    parameters.modelName shouldBe model
  }

}

object SmallScoreSpec {

  case class ScoreParameters(
    modelName: String,
    modelConfig: Option[Config],
    searchId: Int,
    labelCol: String,
    predictionCol: String,
    keywords: Map[String, String],
    utils: PluginUtils
  )

  private val parameters: Stream[Promise[ScoreParameters]] =
    Stream.continually(Promise())

  def parametersToComplete: Promise[ScoreParameters] =
    parameters.dropWhile(_.isCompleted).head

}