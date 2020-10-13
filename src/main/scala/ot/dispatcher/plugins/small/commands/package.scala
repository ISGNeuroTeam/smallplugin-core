package ot.dispatcher.plugins.small


import java.nio.file.Paths

import com.typesafe.config.ConfigException.BadValue
import com.typesafe.config.{Config, ConfigFactory}

import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

package object commands {

  private val classNameSeparator: Char = '@'

  private[commands] def getAlgorithmClassName(config: Config, method: String)(algorithm: String): Try[String]  =
    Try(
      config
        .getConfig(method)
        .getString(algorithm)
    )

  private[commands] def getAlgorithmDetails(description: String): Try[(Option[String], String)] =
    description.split(classNameSeparator) match {

      case Array(configName, className) if configName.nonEmpty && className.nonEmpty =>
        Success(Some(configName), className)

      case Array(className) if className.nonEmpty =>
        Success(None, className)

      case _ =>
        Failure(new BadValue("", description))
    }

  private[commands] def algorithmConfigLoader(configBaseDir: String)(configFileName: String): Try[Config] =
    Try(
      ConfigFactory.parseURL(
        Paths
          .get(configBaseDir)
          .resolve(configFileName)
          .toUri
          .toURL
      )
    )

  private[commands] def getModelInstance[A](classLoader: ClassLoader): String => Try[A] =
    className => Try {
      val mirror = universe.runtimeMirror(classLoader)
      val moduleSymbol = mirror.staticModule(className)
      val moduleMirror = mirror.reflectModule(moduleSymbol)
      moduleMirror.instance.asInstanceOf[A]
    }
}
