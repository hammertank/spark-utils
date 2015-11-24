package my.spark.connection

trait ConnectionPool[C] {
  def borrowConnection(): C
  def returnConnection(c: C)
}