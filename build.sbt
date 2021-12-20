name := "smallplugin-core"

description := "SMaLL plugin core"

organization := "ot.dispatcher.plugins.small"

version := "3.0.0"

scalaVersion := "2.12.10"

lazy val dependencies = new {

  private val smallPluginSdkVersion = "1.0.0"
  private val sparkVersion = "3.1.2"

  val smallPluginSdk = "ot.dispatcher.plugins.small" % "smallplugin-sdk_2.12" % smallPluginSdkVersion % Compile
  val sparkMlLib = "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided
}

libraryDependencies ++= Seq(
  dependencies.smallPluginSdk,
  dependencies.sparkMlLib
)

Test / parallelExecution := false
