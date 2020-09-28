name := "small-plugin-core"

version := "0.0.0"

scalaVersion := "2.11.12"

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.1.0"% Compile

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1"

parallelExecution in Test := false
