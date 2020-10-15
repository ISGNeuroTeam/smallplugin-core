name := "smallPlugin-core"

version := "2.1.0"

scalaVersion := "2.11.12"

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.1.0"% Compile

libraryDependencies += "ot.dispatcher.plugins.small" % "smallplugin-sdk_2.11" % "0.0.0" % Compile

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1"

parallelExecution in Test := false
