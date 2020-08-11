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
        |{"_time":1,"a":1,"b":1,"c":2,"target":1,"regression_gb--1_prediction":"1"},
        |{"_time":1,"a":2,"b":1,"c":2,"target":2,"regression_gb--1_prediction":"20"},
        |{"_time":1,"a":3,"b":1,"c":2,"target":3,"regression_gb--1_prediction":"7"},
        |{"_time":1,"a":4,"b":1,"c":2,"target":4,"regression_gb--1_prediction":"4"},
        |{"_time":1,"a":5,"b":1,"c":2,"target":5,"regression_gb--1_prediction":"5"},
        |{"_time":1,"a":6,"b":1,"c":2,"target":6,"regression_gb--1_prediction":"5"},
        |{"_time":1,"a":7,"b":2,"c":2,"target":7,"regression_gb--1_prediction":"18"},
        |{"_time":1,"a":8,"b":2,"c":2,"target":16,"regression_gb--1_prediction":"16"},
        |{"_time":1,"a":9,"b":2,"c":2,"target":18,"regression_gb--1_prediction":"2"},
        |{"_time":1,"a":10,"b":2,"c":2,"target":20,"regression_gb--1_prediction":"20"},
        |{"_time":1,"a":11,"b":2,"c":2,"target":22,"regression_gb--1_prediction":"22"},
        |{"_time":1,"a":12,"b":2,"c":2,"target":24,"regression_gb--1_prediction":"26"},
        |{"_time":1,"a":13,"b":2,"c":2,"target":26,"regression_gb--1_prediction":"26"},
        |{"_time":1,"a":14,"b":2,"c":2,"target":28,"regression_gb--1_prediction":"3"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Command: | apply gradient_boosting") {
    val queryFit = SimpleQuery(""" regression_gb target from a b c into gbr_test""")
    val commandFit = new SmallFit(queryFit, utils)

    val queryApply = SimpleQuery("""gbr_test""")
    val commandApply = new SmallApply(queryApply, utils)

    val actual = execute(commandFit, commandApply)
    val expected =
      """[
        |{"_time":1,"a":1,"b":1,"c":2,"target":1,"gbr_test_prediction":"1"},
        |{"_time":1,"a":2,"b":1,"c":2,"target":2,"gbr_test_prediction":"20"},
        |{"_time":1,"a":3,"b":1,"c":2,"target":3,"gbr_test_prediction":"7"},
        |{"_time":1,"a":4,"b":1,"c":2,"target":4,"gbr_test_prediction":"4"},
        |{"_time":1,"a":5,"b":1,"c":2,"target":5,"gbr_test_prediction":"5"},
        |{"_time":1,"a":6,"b":1,"c":2,"target":6,"gbr_test_prediction":"5"},
        |{"_time":1,"a":7,"b":2,"c":2,"target":7,"gbr_test_prediction":"18"},
        |{"_time":1,"a":8,"b":2,"c":2,"target":16,"gbr_test_prediction":"16"},
        |{"_time":1,"a":9,"b":2,"c":2,"target":18,"gbr_test_prediction":"2"},
        |{"_time":1,"a":10,"b":2,"c":2,"target":20,"gbr_test_prediction":"20"},
        |{"_time":1,"a":11,"b":2,"c":2,"target":22,"gbr_test_prediction":"22"},
        |{"_time":1,"a":12,"b":2,"c":2,"target":24,"gbr_test_prediction":"26"},
        |{"_time":1,"a":13,"b":2,"c":2,"target":26,"gbr_test_prediction":"26"},
        |{"_time":1,"a":14,"b":2,"c":2,"target":28,"gbr_test_prediction":"3"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | fit regression into ") {
    val queryFit = SimpleQuery("""regression_gb maxDepth=5 learningRate=0.05 iterationSubsample=0.5 subsetStrategy=auto lossType=absolute target from a b c into model1""")
    val commandFit = new SmallFit(queryFit, utils)
    val actual = execute(commandFit)
    val expected =
      """[
        |{"_time":1,"a":1,"b":1,"c":2,"target":1,"model1_prediction":"1"},
        |{"_time":1,"a":2,"b":1,"c":2,"target":2,"model1_prediction":"20"},
        |{"_time":1,"a":3,"b":1,"c":2,"target":3,"model1_prediction":"7"},
        |{"_time":1,"a":4,"b":1,"c":2,"target":4,"model1_prediction":"26"},
        |{"_time":1,"a":5,"b":1,"c":2,"target":5,"model1_prediction":"16"},
        |{"_time":1,"a":6,"b":1,"c":2,"target":6,"model1_prediction":"6"},
        |{"_time":1,"a":7,"b":2,"c":2,"target":7,"model1_prediction":"6"},
        |{"_time":1,"a":8,"b":2,"c":2,"target":16,"model1_prediction":"16"},
        |{"_time":1,"a":9,"b":2,"c":2,"target":18,"model1_prediction":"18"},
        |{"_time":1,"a":10,"b":2,"c":2,"target":20,"model1_prediction":"20"},
        |{"_time":1,"a":11,"b":2,"c":2,"target":22,"model1_prediction":"22"},
        |{"_time":1,"a":12,"b":2,"c":2,"target":24,"model1_prediction":"24"},
        |{"_time":1,"a":13,"b":2,"c":2,"target":26,"model1_prediction":"26"},
        |{"_time":1,"a":14,"b":2,"c":2,"target":28,"model1_prediction":"24"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 3. Command: | fit gradient_boosting | Categorical values") {
    var dataset: String =
      """[{"_time":1,"a":1, "b": "green","c": 2, "target":1,"class":"1a" },
        | {"_time":1,"a":2, "b": "yellow","c": 2, "target":2, "class":"2b" }]""".stripMargin
    val query = SimpleQuery(""" regression_gb target from a b c into gb_test""")
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
    val query = SimpleQuery(""" regression_gb target from a b c into gb_test""")
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
    val queryFit = SimpleQuery(""" regression_gb target from a b c into gb_test""")
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
