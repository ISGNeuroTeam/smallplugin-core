package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.{SmallFit, SmallSave}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class SmallSaveTest extends CommandTest {
  val dataset: String =
    """[{"_time":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","a":1, "b": 2, "c": 2, "target":1,"class":"1a" },
      |{"_time":1,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","a":2, "b": 3,"c": 2, "target":2, "class":"2b" }]""".stripMargin

  test("Test 1. Command: |fit than save") {


    val queryFit1 = SimpleQuery(""" regression target from a b c into model1""")
    val commandFit1 = new SmallFit(queryFit1, utils)

    val queryFit2 = SimpleQuery(""" regression target from a b into model2""")
    val commandFit2 = new SmallFit(queryFit2, utils)

    val querySave= SimpleQuery(""" model1 model2  as fin_model""")
    val commandSave = new SmallSave(querySave, utils)

    val actual = execute(commandFit1, commandFit2, commandSave)
    val expected =
      """[
        |{"a":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","class":"1a","b":2,"c":2,"target":1,"_time":1,"model1_prediction":1.0000000000000004,"model2_prediction":1.0000000000000004},
        |{"a":2,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","class":"2b","b":3,"c":2,"target":2,"_time":1,"model1_prediction":1.9999999999999996,"model2_prediction":1.9999999999999996}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
