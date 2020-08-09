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
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-1.5435020027249835,1.5435020027249835]},"probability_prediction":{"type":1,"values":[0.04364652142729318,0.9563534785727068]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.5435020027249835,-1.5435020027249835]},"probability_prediction":{"type":1,"values":[0.9563534785727067,0.043646521427293306]},"gb_test_prediction":"2b"}
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
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-1.5435020027249835,1.5435020027249835]},"probability_prediction":{"type":1,"values":[0.04364652142729318,0.9563534785727068]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.5435020027249835,-1.5435020027249835]},"probability_prediction":{"type":1,"values":[0.9563534785727067,0.043646521427293306]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | fit gradient_boosting") {
    val query = SimpleQuery(""" gradient_boosting maxDepth=5 learningRate=0.05 iterationSubsample=0.5 class from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-0.5380109019975851,0.5380109019975851]},"probability_prediction":{"type":1,"values":[0.2542595912858634,0.7457404087141366]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[0.4076465254614582,-0.4076465254614582]},"probability_prediction":{"type":1,"values":[0.693236272988668,0.30676372701133203]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
