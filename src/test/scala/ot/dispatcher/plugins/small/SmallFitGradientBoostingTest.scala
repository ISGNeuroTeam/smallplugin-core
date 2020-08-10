package ot.dispatcher.plugins.small

import ot.dispatcher.plugins.small.commands.{SmallApply, SmallFit}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest


class SmallFitGradientBoostingTest extends CommandTest {
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
  // a<5, b<5 -> class 1a
  // a>5, b>5 -> class 2b
  // a<5, b>5 -> class 1a
  // c does not matter


  test("Test 0. Command: | fit gradient_boosting") {
    val query = SimpleQuery(""" gradient_boosting class from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":1,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":4,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":7,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":7,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":9,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":7,"b":5,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":8,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":2,"b":9,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":8,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":7,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"}
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
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":1,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":4,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":7,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":7,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":9,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":7,"b":5,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":8,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-2.2218399428711906,2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.011616091187367285,0.9883839088126327]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":2,"b":9,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":8,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":7,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.2218399428711906,-2.2218399428711906]},"probability_prediction":{"type":1,"values":[0.9883839088126328,0.011616091187367172]},"gb_test_prediction":"1a"}
        |]""".stripMargin

    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | fit gradient_boosting") {
    val query = SimpleQuery(""" gradient_boosting maxDepth=5 learningRate=0.05 iterationSubsample=0.5 subsetStrategy=auto class from a b c into gb_test""")
    val command = new SmallFit(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"_time":1,"a":1,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.0404479507012705,-2.0404479507012705]},"probability_prediction":{"type":1,"values":[0.9833882854671391,0.01661171453286092]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":1,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[2.192166908920286,-2.192166908920286]},"probability_prediction":{"type":1,"values":[0.9876824214507909,0.012317578549209118]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[1.9166066405585733,-1.9166066405585733]},"probability_prediction":{"type":1,"values":[0.9788184006527063,0.02118159934729369]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":4,"b":2,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[1.153573921117774,-1.153573921117774]},"probability_prediction":{"type":1,"values":[0.909467292602452,0.09053270739754804]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":3,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[1.9166066405585733,-1.9166066405585733]},"probability_prediction":{"type":1,"values":[0.9788184006527063,0.02118159934729369]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":7,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-1.878223665582595,1.878223665582595]},"probability_prediction":{"type":1,"values":[0.022833075407707903,0.977166924592292]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":7,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-1.8658512595205479,1.8658512595205479]},"probability_prediction":{"type":1,"values":[0.023391742784099372,0.9766082572159006]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":9,"b":6,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-1.8814787990114634,1.8814787990114634]},"probability_prediction":{"type":1,"values":[0.022688270462594886,0.9773117295374051]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":7,"b":5,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-1.8780319832100618,1.8780319832100618]},"probability_prediction":{"type":1,"values":[0.022841630501756513,0.9771583694982435]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":8,"b":8,"c":2,"class":"2b","target":2,"raw_prediction":{"type":1,"values":[-1.8658512595205479,1.8658512595205479]},"probability_prediction":{"type":1,"values":[0.023391742784099372,0.9766082572159006]},"gb_test_prediction":"2b"},
        |{"_time":1,"a":2,"b":9,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[1.8428852938601863,-1.8428852938601863]},"probability_prediction":{"type":1,"values":[0.9755356700182913,0.024464329981708666]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":3,"b":8,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[1.9010775921331264,-1.9010775921331264]},"probability_prediction":{"type":1,"values":[0.9781648078550165,0.02183519214498353]},"gb_test_prediction":"1a"},
        |{"_time":1,"a":2,"b":7,"c":2,"class":"1a","target":1,"raw_prediction":{"type":1,"values":[1.8412031465600167,-1.8412031465600167]},"probability_prediction":{"type":1,"values":[0.9754552497630733,0.024544750236926705]},"gb_test_prediction":"1a"}
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
