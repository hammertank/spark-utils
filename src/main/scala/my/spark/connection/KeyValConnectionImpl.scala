package my.spark.connection

import redis.clients.jedis.Jedis

class KeyValConnectionImpl(jedis: Jedis) extends KeyValConnectionWithExpire {
  override def get(key: Array[Byte], field: String): Array[Byte] = {
    val fieldBytes = field.getBytes("utf-8")
    jedis.hget(key, fieldBytes)
  }

  override def put(key: Array[Byte], field: String, value: Array[Byte]) {
    val fieldBytes = field.getBytes("utf-8")
    jedis.hset(key, fieldBytes, value)
  }

  override def close {
    RedisConnectionPool.returnConnection(jedis)
  }

  override def expire(key: String, millis: Int) {
    jedis.expire(key, millis)
  }
}