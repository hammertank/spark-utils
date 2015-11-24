package my.spark.connection

object KeyValConnectionPool extends ConnectionPool[KeyValConnection] {

  override def borrowConnection = {
    new KeyValConnectionImpl(RedisConnectionPool.borrowConnection())
  }

  override def returnConnection(c: KeyValConnection) {
    c.close
  }

}