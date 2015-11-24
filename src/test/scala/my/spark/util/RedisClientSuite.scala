package my.spark.util

import org.scalatest.FunSuite
import my.spark.connection.RedisConnectionPool

class RedisClientSuite extends FunSuite {

  test("Functional Test") {
    val jedis = RedisConnectionPool.borrowConnection()
    jedis.set("test", 1.toString())

    assert(jedis.get("test").toInt == 1)
  }
}