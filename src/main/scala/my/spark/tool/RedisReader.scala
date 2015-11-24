package my.spark.tool

import my.spark.connection.RedisConnectionPool
import my.spark.util.SerdeUtils

object RedisReader extends App {

  val key = args(0)

  val field = args(1)

  val jedis = RedisConnectionPool.borrowConnection()

  val valueBytes = jedis.hget(key.getBytes("utf-8"), field.getBytes("utf-8"))

  RedisConnectionPool.returnConnection(jedis)
  
  println(SerdeUtils.convertFromByteArray(valueBytes))
}