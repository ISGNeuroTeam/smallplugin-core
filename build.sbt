name := "smallplugin-core"

description := "SMaLL plugin core"

organization := "ot.dispatcher.plugins.small"

version := "2.1.0"

scalaVersion := "2.11.12"

lazy val dependencies = new {
  private val dispatcherSdkVersion = "1.1.0"
  private val smallPluginSdkVersion = "0.1.0"
  private val sparkVersion = "2.4.3"

  val dispatcherSdk = "ot.dispatcher" % "dispatcher-sdk_2.11" % dispatcherSdkVersion % Provided
  val smallPluginSdk = "ot.dispatcher.plugins.small" % "smallplugin-sdk_2.11" % smallPluginSdkVersion % Compile
  val sparkMlLib = "org.apache.spark" %% "spark-mllib" % sparkVersion % Compile

}

libraryDependencies ++= Seq(
  dependencies.dispatcherSdk,
  dependencies.smallPluginSdk,
  dependencies.sparkMlLib
)

parallelExecution in Test := false
