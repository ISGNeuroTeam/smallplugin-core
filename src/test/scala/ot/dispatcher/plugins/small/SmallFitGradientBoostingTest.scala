package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.{SmallApply, SmallFit}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest


class SmallFitGradientBoostingTest extends CommandTest {
  val dataset: String =
    """[{"_time":1,"a":1, "b": 2,"c": 2, "target":1,"class":"1a" },
      | {"_time":1,"a":2, "b": 3,"c": 2, "target":2, "class":"2b" }]""".stripMargin


  test("Test 0. Command: | fit gradient_boosting") {
    val query = SimpleQuery(""" gradient_boosting class from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 1. Command: | apply random_forest") {
    val queryFit = SimpleQuery(""" gradient_boosting class from a b c into gb_test""")
    val commandFit = new SmallFit(queryFit, utils)

    val queryApply = SimpleQuery("""gb_test""")
    val commandApply = new SmallApply(queryApply, utils)

    val actual = execute(commandFit, commandApply)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | fit gradient_boosting") {
    val query = SimpleQuery(""" gradient_boosting maxDepth=5 learningRate=0.05 iterationSubsample=0.5 class from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-1.2309022156524796,1.2309022156524796]},"probability_prediction":{"type":1,"values":[0.07857958822766717,0.9214204117723328]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.265880152376362,-1.265880152376362]},"probability_prediction":{"type":1,"values":[0.9263385587364967,0.07366144126350327]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 3. Command: | fit gradient_boosting | Categorical values") {
    var dataset: String =
      """[{"_time":1,"a":1, "b": "green","c": 2, "target":1,"class":"1a" },
        | {"_time":1,"a":2, "b": "yellow","c": 2, "target":2, "class":"2b" }]""".stripMargin
    val query = SimpleQuery(""" gradient_boosting class from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 4. Command: | fit gradient_boosting | Empty values") {
    var dataset: String =
      """[{"_time":1,"a":1, "b": "","c": 2, "target":1,"class":"1a" },
        | {"_time":1,"a":2, "b": "","c": 2, "target":2, "class":"2b" }]""".stripMargin
    val query = SimpleQuery(""" gradient_boosting class from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 5. Command: | apply random_forest | New features") {
    var dataset: String =
      """[{"_time":1,"a":1, "b": "2","c": 2, "d": 2, "target":1,"class":"1a" },
        | {"_time":1,"a":2, "b": "3","c": 2, "d": 4, "target":2, "class":"2b" }]""".stripMargin
    val queryFit = SimpleQuery(""" gradient_boosting class from a b c into gb_test""")
    val commandFit = new SmallFit(queryFit, utils)

    val queryApply = SimpleQuery("""gb_test""")
    val commandApply = new SmallApply(queryApply, utils)

    val actual = execute(commandFit, commandApply)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
