package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.{SmallApply, SmallFit}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest


class SmallFitGradientBoostingTest extends CommandTest {
  val dataset: String =
    """[{"_time":1,"a":1, "b": 2, "c": 2, "target":1,"class":"1a" },
      | {"_time":1,"a":2, "b": 3,"c": 2, "target":2, "class":"2b" }]""".stripMargin


  test("Test 0. Command: | fit gradient_boosting") {
    val query = SimpleQuery(""" gradient_boosting class from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"a":1,"class":"1a","b":2,"c":2,"target":1,"_time":1,"raw_prediction":{"type":1,"values":[0.0,9.0]},"probability_prediction":{"type":1,"values":[0.0,1.0]},"gb_test_prediction":"1a"},
        |{"a":2,"class":"2b","b":3,"c":2,"target":2,"_time":1,"raw_prediction":{"type":1,"values":[6.0,3.0]},"probability_prediction":{"type":1,"values":[0.6666666666666666,0.3333333333333333]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 1. Command: | apply random_forest") {
    val queryFit = SimpleQuery(""" random_forest class from a b c into rf_test""")
    val commandFit = new SmallFit(queryFit, utils)

    val queryApply = SimpleQuery("""rf_test""")
    val commandApply = new SmallApply(queryApply, utils)

    val actual = execute(commandFit, commandApply)
    val expected =
      """[
        |{"a":1,"class":"1a","b":2,"c":2,"target":1,"_time":1,"raw_prediction":{"type":1,"values":[0.0,9.0]},"probability_prediction":{"type":1,"values":[0.0,1.0]},"rf_test_prediction":"1a"},
        |{"a":2,"class":"2b","b":3,"c":2,"target":2,"_time":1,"raw_prediction":{"type":1,"values":[6.0,3.0]},"probability_prediction":{"type":1,"values":[0.6666666666666666,0.3333333333333333]},"rf_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | fit random_forest") {
    val query = SimpleQuery(""" random_forest max=2 class from a b c into rf_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"a":1,"class":"1a","b":2,"c":2,"target":1,"_time":1,"raw_prediction":{"type":1,"values":[0.0,9.0]},"probability_prediction":{"type":1,"values":[0.0,1.0]},"rf_test_prediction":"1a"},
        |{"a":2,"class":"2b","b":3,"c":2,"target":2,"_time":1,"raw_prediction":{"type":1,"values":[6.0,3.0]},"probability_prediction":{"type":1,"values":[0.6666666666666666,0.3333333333333333]},"rf_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
