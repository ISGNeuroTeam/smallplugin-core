package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.{SmallApply, SmallFit}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest


class SmallFitRandomForestClassifierTest extends CommandTest {
  val dataset: String =
    """[
      |{"_time":1,"a":1, "b":2, "c":2, "target":1, "class":"1a" },
      |{"_time":1,"a":2, "b":1, "c":2, "target":1, "class":"1a" },
      |{"_time":1,"a":3, "b":3, "c":2, "target":1, "class":"1a" },
      |{"_time":1,"a":4, "b":2, "c":2, "target":1, "class":"1a" },
      |{"_time":1,"a":3, "b":3, "c":2, "target":1, "class":"1a" },
      |{"_time":1,"a":7, "b":6, "c":2, "target":2, "class":"2b" },
      |{"_time":1,"a":8, "b":7, "c":2, "target":2, "class":"2b" },
      |{"_time":1,"a":9, "b":6, "c":2, "target":2, "class":"2b" },
      |{"_time":1,"a":7, "b":5, "c":2, "target":2, "class":"2b" },
      |{"_time":1,"a":8, "b":8, "c":2, "target":2, "class":"2b" },
      |{"_time":1,"a":2, "b":9, "c":2, "target":1, "class":"1a" },
      |{"_time":1,"a":3, "b":8, "c":2, "target":1, "class":"1a" },
      |{"_time":1,"a":2, "b":7, "c":2, "target":1, "class":"1a" }
      | ]""".stripMargin


  test("Test 0. Command: | fit random_forest") {
    val query = SimpleQuery(""" classification_random_forest class from a b c into rf_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[100.0,0.0]},"probability_prediction":{"type":1,"values":[1.0,0.0]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":1,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[100.0,0.0]},"probability_prediction":{"type":1,"values":[1.0,0.0]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[97.0,3.0]},"probability_prediction":{"type":1,"values":[0.97,0.03]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":4,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[74.0,26.0]},"probability_prediction":{"type":1,"values":[0.74,0.26]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[97.0,3.0]},"probability_prediction":{"type":1,"values":[0.97,0.03]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":7,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.45,98.55]},"probability_prediction":{"type":1,"values":[0.014499999999999999,0.9854999999999999]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":7,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[3.45,96.55]},"probability_prediction":{"type":1,"values":[0.0345,0.9655]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":9,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.45,98.55]},"probability_prediction":{"type":1,"values":[0.014499999999999999,0.9854999999999999]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":7,"b":5,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[0.45,99.55]},"probability_prediction":{"type":1,"values":[0.0045000000000000005,0.9954999999999999]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":8,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[4.2,95.8]},"probability_prediction":{"type":1,"values":[0.042,0.958]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":2,"b":9,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[96.2,3.8]},"probability_prediction":{"type":1,"values":[0.9620000000000001,0.038]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":8,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[90.2,9.8]},"probability_prediction":{"type":1,"values":[0.902,0.098]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":7,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[94.45,5.55]},"probability_prediction":{"type":1,"values":[0.9445,0.0555]},"rf_test_prediction":"1a"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }


  test("Test 1. Command: | apply classification_random_forest") {
    val queryFit = SimpleQuery(""" classification_random_forest class from a b c into rf_test""")
    val commandFit = new SmallFit(queryFit, utils)

    val queryApply = SimpleQuery("""rf_test""")
    val commandApply = new SmallApply(queryApply, utils)

    val actual = execute(commandFit, commandApply)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[100.0,0.0]},"probability_prediction":{"type":1,"values":[1.0,0.0]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":1,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[100.0,0.0]},"probability_prediction":{"type":1,"values":[1.0,0.0]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[97.0,3.0]},"probability_prediction":{"type":1,"values":[0.97,0.03]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":4,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[74.0,26.0]},"probability_prediction":{"type":1,"values":[0.74,0.26]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[97.0,3.0]},"probability_prediction":{"type":1,"values":[0.97,0.03]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":7,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.45,98.55]},"probability_prediction":{"type":1,"values":[0.014499999999999999,0.9854999999999999]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":7,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[3.45,96.55]},"probability_prediction":{"type":1,"values":[0.0345,0.9655]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":9,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.45,98.55]},"probability_prediction":{"type":1,"values":[0.014499999999999999,0.9854999999999999]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":7,"b":5,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[0.45,99.55]},"probability_prediction":{"type":1,"values":[0.0045000000000000005,0.9954999999999999]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":8,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[4.2,95.8]},"probability_prediction":{"type":1,"values":[0.042,0.958]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":2,"b":9,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[96.2,3.8]},"probability_prediction":{"type":1,"values":[0.9620000000000001,0.038]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":8,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[90.2,9.8]},"probability_prediction":{"type":1,"values":[0.902,0.098]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":7,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[94.45,5.55]},"probability_prediction":{"type":1,"values":[0.9445,0.0555]},"rf_test_prediction":"1a"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | fit classification_random_forest") {
    val query = SimpleQuery(""" classification_random_forest max=2 numTrees=150 subsetStrategy=all impurity=entropy class from a b c into rf_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[150.0,0.0]},"probability_prediction":{"type":1,"values":[1.0,0.0]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":1,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[150.0,0.0]},"probability_prediction":{"type":1,"values":[1.0,0.0]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[147.0,3.0]},"probability_prediction":{"type":1,"values":[0.98,0.02]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":4,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[100.0,50.0]},"probability_prediction":{"type":1,"values":[0.6666666666666666,0.3333333333333333]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[147.0,3.0]},"probability_prediction":{"type":1,"values":[0.98,0.02]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":7,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.0,149.0]},"probability_prediction":{"type":1,"values":[0.006666666666666667,0.9933333333333333]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":7,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.0,149.0]},"probability_prediction":{"type":1,"values":[0.006666666666666667,0.9933333333333333]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":9,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.0,149.0]},"probability_prediction":{"type":1,"values":[0.006666666666666667,0.9933333333333333]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":7,"b":5,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.0,149.0]},"probability_prediction":{"type":1,"values":[0.006666666666666667,0.9933333333333333]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":8,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[1.0,149.0]},"probability_prediction":{"type":1,"values":[0.006666666666666667,0.9933333333333333]},"rf_test_prediction":"2b"},
        |{"_time":1,"a":2,"b":9,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[150.0,0.0]},"probability_prediction":{"type":1,"values":[1.0,0.0]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":8,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[147.0,3.0]},"probability_prediction":{"type":1,"values":[0.98,0.02]},"rf_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":7,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[150.0,0.0]},"probability_prediction":{"type":1,"values":[1.0,0.0]},"rf_test_prediction":"1a"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  ignore("Test 3. Command: | fit gradient_boosting | Categorical values") {
    var dataset: String =
      """[{"_time":1,"a":1, "b": "green","c": 2, "target":1,"class":"1a" },
        | {"_time":1,"a":2, "b": "yellow","c": 2, "target":2, "class":"2b" }]""".stripMargin
    val query = SimpleQuery(""" classification_gradient_boosting class from a b c into gb_test""")
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
    val query = SimpleQuery(""" classification_gradient_boosting class from a b c into gb_test""")
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
    val queryFit = SimpleQuery(""" classification_gradient_boosting class from a b c into gb_test""")
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
