package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.{SmallApply, SmallFit}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest


class SmallFitRandomForestRegressorfTest extends CommandTest {
  val dataset: String =
    """[
      |{"_time":1,"a":1, "b":1, "c":2, "target":1 },
      |{"_time":1,"a":2, "b":1, "c":2, "target":2 },
      |{"_time":1,"a":3, "b":1, "c":2, "target":3 },
      |{"_time":1,"a":4, "b":1, "c":2, "target":4 },
      |{"_time":1,"a":5, "b":1, "c":2, "target":5 },
      |{"_time":1,"a":6, "b":1, "c":2, "target":6 },
      |{"_time":1,"a":7, "b":1, "c":2, "target":7 },
      |{"_time":1,"a":8, "b":2, "c":2, "target":16 },
      |{"_time":1,"a":9, "b":2, "c":2, "target":18 },
      |{"_time":1,"a":10, "b":2, "c":2, "target":20 },
      |{"_time":1,"a":11, "b":2, "c":2, "target":22 },
      |{"_time":1,"a":12, "b":2, "c":2, "target":24 },
      |{"_time":1,"a":13, "b":2, "c":2, "target":26 },
      |{"_time":1,"a":14, "b":2, "c":2, "target":28 }
      | ]""".stripMargin


  test("Test 0. Command: | fit random_forest") {
    val query = SimpleQuery(""" regression_random_forest target from a b into rf_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":1,"c":2,"target":1,"rf_test_prediction":"1"},
        |{"_time":1,"a":2,"b":1,"c":2,"target":2,"rf_test_prediction":"1"},
        |{"_time":1,"a":3,"b":1,"c":2,"target":3,"rf_test_prediction":"1"},
        |{"_time":1,"a":4,"b":1,"c":2,"target":4,"rf_test_prediction":"5"},
        |{"_time":1,"a":5,"b":1,"c":2,"target":5,"rf_test_prediction":"6"},
        |{"_time":1,"a":6,"b":1,"c":2,"target":6,"rf_test_prediction":"6"},
        |{"_time":1,"a":7,"b":1,"c":2,"target":7,"rf_test_prediction":"1"},
        |{"_time":1,"a":8,"b":2,"c":2,"target":16,"rf_test_prediction":"6"},
        |{"_time":1,"a":9,"b":2,"c":2,"target":18,"rf_test_prediction":"6"},
        |{"_time":1,"a":10,"b":2,"c":2,"target":20,"rf_test_prediction":"5"},
        |{"_time":1,"a":11,"b":2,"c":2,"target":22,"rf_test_prediction":"16"},
        |{"_time":1,"a":12,"b":2,"c":2,"target":24,"rf_test_prediction":"16"},
        |{"_time":1,"a":13,"b":2,"c":2,"target":26,"rf_test_prediction":"5"},
        |{"_time":1,"a":14,"b":2,"c":2,"target":28,"rf_test_prediction":"1"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 1. Command: | apply random_forest") {
    val queryFit = SimpleQuery(""" regression_random_forest target from a b c into rf_test""")
    val commandFit = new SmallFit(queryFit, utils)

    val queryApply = SimpleQuery("""rf_test""")
    val commandApply = new SmallApply(queryApply, utils)

    val actual = execute(commandFit, commandApply)
    val expected =
      """[
        |{"_time":1,"a":1,"b":1,"c":2,"target":1,"rf_test_prediction":"1"},
        |{"_time":1,"a":2,"b":1,"c":2,"target":2,"rf_test_prediction":"1"},
        |{"_time":1,"a":3,"b":1,"c":2,"target":3,"rf_test_prediction":"1"},
        |{"_time":1,"a":4,"b":1,"c":2,"target":4,"rf_test_prediction":"6"},
        |{"_time":1,"a":5,"b":1,"c":2,"target":5,"rf_test_prediction":"6"},
        |{"_time":1,"a":6,"b":1,"c":2,"target":6,"rf_test_prediction":"6"},
        |{"_time":1,"a":7,"b":1,"c":2,"target":7,"rf_test_prediction":"1"},
        |{"_time":1,"a":8,"b":2,"c":2,"target":16,"rf_test_prediction":"6"},
        |{"_time":1,"a":9,"b":2,"c":2,"target":18,"rf_test_prediction":"6"},
        |{"_time":1,"a":10,"b":2,"c":2,"target":20,"rf_test_prediction":"6"},
        |{"_time":1,"a":11,"b":2,"c":2,"target":22,"rf_test_prediction":"5"},
        |{"_time":1,"a":12,"b":2,"c":2,"target":24,"rf_test_prediction":"5"},
        |{"_time":1,"a":13,"b":2,"c":2,"target":26,"rf_test_prediction":"6"},
        |{"_time":1,"a":14,"b":2,"c":2,"target":28,"rf_test_prediction":"1"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | fit random_forest") {
    val query = SimpleQuery(""" regression_random_forest max=2 numTrees=150 subsetStrategy=all target from a b c into rf_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":1,"c":2,"target":1,"rf_test_prediction":"1"},
        |{"_time":1,"a":2,"b":1,"c":2,"target":2,"rf_test_prediction":"20"},
        |{"_time":1,"a":3,"b":1,"c":2,"target":3,"rf_test_prediction":"20"},
        |{"_time":1,"a":4,"b":1,"c":2,"target":4,"rf_test_prediction":"24"},
        |{"_time":1,"a":5,"b":1,"c":2,"target":5,"rf_test_prediction":"5"},
        |{"_time":1,"a":6,"b":1,"c":2,"target":6,"rf_test_prediction":"6"},
        |{"_time":1,"a":7,"b":1,"c":2,"target":7,"rf_test_prediction":"1"},
        |{"_time":1,"a":8,"b":2,"c":2,"target":16,"rf_test_prediction":"6"},
        |{"_time":1,"a":9,"b":2,"c":2,"target":18,"rf_test_prediction":"1"},
        |{"_time":1,"a":10,"b":2,"c":2,"target":20,"rf_test_prediction":"5"},
        |{"_time":1,"a":11,"b":2,"c":2,"target":22,"rf_test_prediction":"26"},
        |{"_time":1,"a":12,"b":2,"c":2,"target":24,"rf_test_prediction":"24"},
        |{"_time":1,"a":13,"b":2,"c":2,"target":26,"rf_test_prediction":"16"},
        |{"_time":1,"a":14,"b":2,"c":2,"target":28,"rf_test_prediction":"20"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 3. Command: | fit regression_random_forest | Categorical values") {
    var dataset: String =
      """[{"_time":1,"a":1, "b": "green","c": 2, "target":1,"class":"1a" },
        | {"_time":1,"a":2, "b": "yellow","c": 2, "target":2, "class":"2b" }]""".stripMargin
    val query = SimpleQuery(""" regression_random_forest target from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 4. Command: | fit regression_random_forest | Empty values") {
    var dataset: String =
      """[{"_time":1,"a":1, "b": "","c": 2, "target":1,"class":"1a" },
        | {"_time":1,"a":2, "b": "","c": 2, "target":2, "class":"2b" }]""".stripMargin
    val query = SimpleQuery(""" regression_random_forest target from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":3,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"2b"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 5. Command: | apply regression_random_forest | New features") {
    var dataset: String =
      """[{"_time":1,"a":1, "b": "2","c": 2, "d": 2, "target":1,"class":"1a" },
        | {"_time":1,"a":2, "b": "3","c": 2, "d": 4, "target":2, "class":"2b" }]""".stripMargin
    val queryFit = SimpleQuery(""" regression_random_forest target from a b c into gb_test""")
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
