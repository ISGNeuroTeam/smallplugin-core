package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.{SmallApply, SmallFit, SmallSave}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class SmallApplyTest extends CommandTest {
  val dataset: String =
    """[{"_time":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","a":1, "b": 2, "c": 2, "target":1,"class":"1a" },
      |{"_time":1,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","a":2, "b": 3,"c": 2, "target":2, "class":"2b" }]""".stripMargin

  test("Test 0. Command: |fit than apply ") {
    val queryFit = SimpleQuery("""regression target from a b c into model1 """)
    val commandFit = new SmallFit(queryFit, utils)

    val queryApply = SimpleQuery("""model1""")
    val commandApply = new SmallApply(queryApply, utils)
    val actual = execute(commandFit, commandApply)

    val expected =
      """[
        |{"a":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","class":"1a","b":2,"c":2,"target":1,"_time":1,"model1_prediction":1.0000000000000004},
        |{"a":2,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","class":"2b","b":3,"c":2,"target":2,"_time":1,"model1_prediction":1.9999999999999996}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | applying saved model  ") {
    val queryReg1 = SimpleQuery("""regression target from a b c into model1""")
    val commandReg1 = new SmallFit(queryReg1, utils)

    val queryReg2 = SimpleQuery(""" regression target from a b into model2""")
    val commandReg2 = new SmallFit(queryReg2, utils)

    val querySave = SimpleQuery("""model1 model2  as fin_model""")
    val commandSave = new SmallSave(querySave, utils)
    execute(commandReg1, commandReg2, commandSave)

    val queryApply = SimpleQuery("""fin_model""")
    val commandApply = new SmallApply(queryApply, utils)
    val actual = execute(commandApply)

    val expected =
      """[
        |{"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"model1_prediction":1.0000000000000004,"model2_prediction":1.0000000000000004,"fin_model_prediction":1.0000000000000004},
        |{"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"model1_prediction":1.9999999999999996,"model2_prediction":1.9999999999999996,"fin_model_prediction":1.9999999999999996}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | apply predict ") {
    val query = SimpleQuery("""predict target future=10 period=1""")
    val command = new SmallApply(query, utils)
    val actual = execute(command)

    val expected =
      """[
        |{"a":1,"_raw":"{\"a\":1, \"b\": 2, \"c\": 2, \"target\":1, \"class\":\"1a\" }","class":"1a","b":2,"c":2,"target":1,"_time":1,"prediction":1.5},
        |{"a":2,"_raw":"{\"a\":2, \"b\": 3,\"c\": 2, \"target\":2, \"class\":\"2b\"}","class":"2b","b":3,"c":2,"target":2,"_time":1,"prediction":1.5},
        |{"_time":86401,"prediction":1.5},
        |{"_time":172801,"prediction":1.5},
        |{"_time":259201,"prediction":1.5},
        |{"_time":345601,"prediction":1.5},
        |{"_time":432001,"prediction":1.5},
        |{"_time":518401,"prediction":1.5},
        |{"_time":604801,"prediction":1.5},
        |{"_time":691201,"prediction":1.5},
        |{"_time":777601,"prediction":1.5},
        |{"_time":864001,"prediction":1.5}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
