package ot.dispatcher.plugins.small


import com.typesafe.config.Config

import scala.reflect.runtime.universe
import scala.util.Try

package object commands {

  private[commands] def getAlgorithmClassName(method: String, algorithm: String): Config => Try[String]  =
    config => Try(
      config
        .getConfig(method)
        .getString(algorithm)
    )

  private[commands] def getModelInstance[A](classLoader: ClassLoader): String => Try[A] =
    className => Try {
      val mirror = universe.runtimeMirror(classLoader)
      val moduleSymbol = mirror.staticModule(className)
      val moduleMirror = mirror.reflectModule(moduleSymbol)
      moduleMirror.instance.asInstanceOf[A]
    }
}
