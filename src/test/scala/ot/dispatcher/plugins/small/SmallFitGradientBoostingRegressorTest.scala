package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.{SmallApply, SmallFit}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest


class SmallFitGradientBoostingRegressorTest extends CommandTest {
  val dataset: String =
    """[
      |{"_time":1,"a":1, "b":1, "c":2, "target":1 },
      |{"_time":1,"a":2, "b":1, "c":2, "target":2 },
      |{"_time":1,"a":3, "b":1, "c":2, "target":3 },
      |{"_time":1,"a":4, "b":1, "c":2, "target":4 },
      |{"_time":1,"a":5, "b":1, "c":2, "target":5 },
      |{"_time":1,"a":6, "b":1, "c":2, "target":6 },
      |{"_time":1,"a":7, "b":2, "c":2, "target":7 },
      |{"_time":1,"a":8, "b":2, "c":2, "target":16 },
      |{"_time":1,"a":9, "b":2, "c":2, "target":18 },
      |{"_time":1,"a":10, "b":2, "c":2, "target":20 },
      |{"_time":1,"a":11, "b":2, "c":2, "target":22 },
      |{"_time":1,"a":12, "b":2, "c":2, "target":24 },
      |{"_time":1,"a":13, "b":2, "c":2, "target":26 },
      |{"_time":1,"a":14, "b":2, "c":2, "target":28 }
      | ]""".stripMargin

  test("Test 0. Command: | fit regression") {
    val queryFit = SimpleQuery("""regression_gb target from a b c""")
    val commandFit = new SmallFit(queryFit, utils)
    val actual = execute(commandFit)
    val expected =
      """[
        |{"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"regression--1_prediction":1.0000000000000004},
        |{"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"regression--1_prediction":1.9999999999999996}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | fit regression into ") {
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
