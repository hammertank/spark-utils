package my.spark.connection

import redis.clients.jedis.Jedis

class KeyValConnectionImpl(jedis: Jedis) extends KeyValConnection {
  override def get(key: Array[Byte], field: Array[Byte]): Array[Byte] = {
    jedis.hget(key, field)
  }

  override def put(key: Array[Byte], field: Array[Byte], value: Array[Byte]) {
    jedis.hset(key, field, value)
  }

  override def close {
    RedisConnectionPool.returnConnection(jedis)
  }

  override def expire(key: String, millis: Int) {
    jedis.expire(key, millis)
  }

  override def expire(key: Array[Byte], millis: Int) {
    jedis.expire(key, millis)
  }
}