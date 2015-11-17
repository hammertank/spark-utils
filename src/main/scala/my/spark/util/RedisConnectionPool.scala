package my.spark.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool

/**
 * @author hammertank
 *
 */
object RedisConnectionPool {
  val REDIS_HOST = "redis.host"
  val REDIS_PORT = "redis.port"
  val REDIS_TIMEOUT = "redis.timeout"
  val REDIS_PASSWD = "redis.passwd"

  val redisHost = ConfigUtils.getString(REDIS_HOST, "localhost")
  val redisPort = ConfigUtils.getInt(REDIS_PORT, 6379)
  val redisTimeout = ConfigUtils.getInt(REDIS_TIMEOUT, 3000)
  val redisPasswd = ConfigUtils.getString(REDIS_PASSWD, "")
  private lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort,redisTimeout, redisPasswd)

  def borrowConnection() = {
    pool.getResource
  }

  def returnConnection(jedis: Jedis) {
    pool.returnResourceObject(jedis)
  }

  sys.addShutdownHook {
    println("Execute hook thread: " + this)
    pool.destroy()
  }
}