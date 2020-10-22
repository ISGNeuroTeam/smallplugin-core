package ot.dispatcher.plugins.small


import java.nio.file.Paths

import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}

package object commands {

  private val parametersSeparator: Char = '@'

  private[commands] def getAlgorithmParameters(config: Config, method: String)(algorithm: String): Try[String]  =
    Try(
      config
        .getConfig(method)
        .getString(algorithm)
    )

  private[commands] def parseAlgorithmParameters(parameters: String): Try[(Option[String], String)] =
    parameters split parametersSeparator match {

      case Array(configName, className) if configName.nonEmpty && className.nonEmpty =>
        Success(Some(configName), className)

      case Array(className) if className.nonEmpty =>
        Success(None, className)

      case _ =>
        Failure(
          new IllegalArgumentException(s"Can not parse algorithm parameters in '$parameters'.")
        )
    }

  private[commands] def algorithmConfigLoader(config: Config)(configFileName: String): Try[Config] =
    Try {

      val configBasePath =
        config.getString("configBasePath")

      val url = Paths
        .get(configBasePath)
        .resolve(configFileName)
        .toUri
        .toURL

      val options = ConfigParseOptions
        .defaults()
        .setAllowMissing(false)

      ConfigFactory.parseURL(url, options)
    }

  private[commands] def getModelInstance[A](classLoader: ClassLoader): String => Try[A] =
    className => Try {
      val mirror = universe.runtimeMirror(classLoader)
      val moduleSymbol = mirror.staticModule(className)
      val moduleMirror = mirror.reflectModule(moduleSymbol)
      moduleMirror.instance.asInstanceOf[A]
    }
}
