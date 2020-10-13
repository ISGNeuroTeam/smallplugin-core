package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.SmallScore
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class SmallScoreTest extends CommandTest{
  val dataset: String =
    """[{"a":1.9, "b": 2.0 },
      |{"a":2.5, "b": 3.1 }]""".stripMargin

  test("Test 0. Command: | score unknown_metric") {
    val queryScore = SimpleQuery("""score unknown_metric a vs b""")
    val commandScore = new SmallScore(queryScore, utils)

    val thrown = intercept[Exception] {
      execute(commandScore)
    }
    assert(thrown.getMessage.startsWith("Can not get metric 'unknown_metric' to score."))
  }

  test("Test 1. Command: | score mse") {
    val queryScore = SimpleQuery("""score mse a vs b""")
    val commandScore = new SmallScore(queryScore, utils)
    val actual = execute(commandScore)
    val expected =
      """[
        |{"mse": 0.18500000000000005}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.1. Command: | score rmse") {
    val queryScore = SimpleQuery("""score rmse a vs b""")
    val commandScore = new SmallScore(queryScore, utils)
    val actual = execute(commandScore)
    val expected =
      """[
        |{"rmse": 0.4301162633521314}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.2. Command: | score mae") {
    val queryScore = SimpleQuery("""score mae a vs b""")
    val commandScore = new SmallScore(queryScore, utils)
    val actual = execute(commandScore)
    val expected =
      """[
        |{"mae": 0.3500000000000001}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.3. Command: | score mape") {
    val queryScore = SimpleQuery("""score mape a vs b""")
    val commandScore = new SmallScore(queryScore, utils)
    val actual = execute(commandScore)
    val expected =
      """[
        |{"mape": 0.14631578947368426}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.4. Command: | score smape") {
    val queryScore = SimpleQuery("""score smape a vs b""")
    val commandScore = new SmallScore(queryScore, utils)
    val actual = execute(commandScore)
    val expected =
      """[
        |{"smape": 13.278388278388285}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1.5. Command: | score r2") {
    val queryScore = SimpleQuery("""score r2 a vs b""")
    val commandScore = new SmallScore(queryScore, utils)
    val actual = execute(commandScore)
    val expected =
      """[
        |{"r2": -1.0555555555555558}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test1.6. Command: | score r2adj") {
    val queryScore = SimpleQuery("""score r2_adj a vs b with 5""")
    val commandScore = new SmallScore(queryScore, utils)
    val actual = execute(commandScore)
    val expected =
      """[
        |{"r2_adj": 1.6851851851851851}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 2. Command: | score without labelCol"){
    val queryScore = SimpleQuery("""score rmse""")
    val thrown = intercept[Exception] {
      val commandScore = new SmallScore(queryScore, utils)
    }
    assert(thrown.getMessage.endsWith("Label column name is not specified"))
  }

  test("Test 2.1. Command: | score without labelCol"){
    val queryScore = SimpleQuery("""score rmse vs b""")
    val thrown = intercept[Exception] {
      val commandScore = new SmallScore(queryScore, utils)
    }
    assert(thrown.getMessage.endsWith("Label column name is not specified"))
  }

  test("Test 3. Command: | score without predictionCol") {
    val queryScore = SimpleQuery("""score rmse a vs""")
    val thrown = intercept[Exception] {
      val commandScore = new SmallScore(queryScore, utils)
    }
    assert(thrown.getMessage.endsWith("Prediction column name is not specified"))
  }

  test("Test 4. Command: | score without vs") {
    val queryScore = SimpleQuery("""score rmse a b""")
    val thrown = intercept[Exception] {
      val commandScore = new SmallScore(queryScore, utils)
    }
    assert(thrown.getMessage.endsWith("Syntax error, the preposition 'vs' is required between label and predictions columns"))
  }

}
