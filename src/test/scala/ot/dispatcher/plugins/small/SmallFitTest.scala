package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.SmallFit
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class SmallFitTest extends CommandTest {
  val dataset: String =
    """[{"_time":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","a":1, "b": 2, "c": 2, "target":1,"class":"1a" },
      |{"_time":1,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","a":2, "b": 3,"c": 2, "target":2, "class":"2b" }]""".stripMargin

  test("Test 0. Command: | fit regression") {
    val queryFit = SimpleQuery("""regression target from a b c""")
    val commandFit = new SmallFit(queryFit, utils)
    val actual = execute(commandFit)
    val expected =
      """[
        |{"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"regression--1_prediction":1.0000000000000004},
        |{"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"regression--1_prediction":1.9999999999999996}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 0.1 Command: | fit clustering") {
    val queryFit = SimpleQuery("""clustering num=2-10 from a b c into model1""")
    val commandFit = new SmallFit(queryFit, utils)
    val actual = execute(commandFit)

    val expected =
      """[
        |{"a":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","class":"1a","b":2,"c":2,"target":1,"_time":1,"prediction":1},
        |{"a":2,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","class":"2b","b":3,"c":2,"target":2,"_time":1,"prediction":0}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 0.2 Command: | fit classification") {
    val queryFit = SimpleQuery("""classification class from a b c into model5""")
    val commandFit = new SmallFit(queryFit, utils)
    val actual = execute(commandFit)

    val expected =
      """[
        |{"a":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","class":"1a","b":2,"c":2,"target":1,"_time":1,"rawPrediction":{"type":1,"values":[-18.177596079821207,18.177596079821207]},"probability":{"type":1,"values":[1.2751765650049199E-8,0.9999999872482342]},"model5_index_prediction":1.0,"model5_prediction":"1a"},
        |{"a":2,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","class":"2b","b":3,"c":2,"target":2,"_time":1,"rawPrediction":{"type":1,"values":[18.539362807613465,-18.539362807613465]},"probability":{"type":1,"values":[0.999999991119099,8.880901041128542E-9]},"model5_index_prediction":0.0,"model5_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | fit unknown_algo") {
    val queryFit = SimpleQuery("""unknown_algo target from""")
    val commandFit = new SmallFit(queryFit, utils)

    val thrown = intercept[Exception] {
      execute(commandFit)
    }
    assert(thrown.getMessage.endsWith("Algorithm with name 'unknown_algo'  is unsupported at this moment"))
  }

  test("Test 1.1 Command: | fit unknown_algo") {
    val queryFit = SimpleQuery("""regression""")

    val thrown = intercept[Exception] {
      val commandFit = new SmallFit(queryFit, utils)
    }
    assert(thrown.getMessage.endsWith("Target column name is not specified"))
  }

  test("Test 2. Command: | fit regression into ") {
    val queryFit = SimpleQuery("""regression target from a b c into model1""")
    val commandFit = new SmallFit(queryFit, utils)
    val actual = execute(commandFit)
    val expected =
      """[
        |{"a":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","class":"1a","b":2,"c":2,"target":1,"_time":1,"model1_prediction":1.0000000000000004},
        |{"a":2,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","class":"2b","b":3,"c":2,"target":2,"_time":1,"model1_prediction":1.9999999999999996}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
