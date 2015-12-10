package my.spark.connection

import org.scalatest.FunSuite

class RedisClientSuite extends FunSuite {

  test("Functional Test") {
    val jedis = RedisConnectionPool.borrowConnection()
    jedis.set("test", 1.toString())

    assert(jedis.get("test").toInt == 1)
  }
}