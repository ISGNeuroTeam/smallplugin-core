name := "smallplugin-core"

description := "SMaLL plugin core"

organization := "ot.dispatcher.plugins.small"

version := "2.1.7"

scalaVersion := "2.11.12"

resolvers += ("Sonatype OSS Snapshots" at (sys.env.getOrElse("NEXUS_OTP_URL_HTTPS","http://storage.dev.isgneuro.com")
  + "/repository/ot.platform-sbt-releases/")).withAllowInsecureProtocol(true)

lazy val dependencies = new {
  private val smallPluginSdkVersion = "0.3.0"
  private val sparkVersion = "2.4.3"

  val smallPluginSdk = "ot.dispatcher.plugins.small" % "smallplugin-sdk_2.11" % smallPluginSdkVersion % Compile
  val sparkMlLib = "org.apache.spark" %% "spark-mllib" % sparkVersion % Compile

}

libraryDependencies ++= Seq(
  dependencies.smallPluginSdk,
  dependencies.sparkMlLib
)

Test / parallelExecution := false
