package my.spark.util

import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import org.apache.spark.Logging

/**
 * @author hammertank
 *
 */
object ConfigUtils extends Logging {
  val config = ConfigFactory.load

  def getString(key: String, default: String): String = {
    try {
      config.getString(key)
    } catch {
      case ex: ConfigException => default
    }
  }

  def getInt(key: String, default: Int): Int = {
    try {
      config.getInt(key)
    } catch {
      case ex: ConfigException => default
    }
  }

  def getDouble(key: String, default: Double): Double = {
    try {
      config.getDouble(key)
    } catch {
      case ex: ConfigException => default
    }
  }
  
  def getBoolean(key: String, default: Boolean): Boolean = {
     try {
      config.getBoolean(key)
    } catch {
      case ex: ConfigException => default
    }
  }
}