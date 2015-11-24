package my.spark.connection

trait KeyValConnection {

  def close

  def get(key: Array[Byte], field: String): Array[Byte]
  def put(key: Array[Byte], field: String, value: Array[Byte])
}

trait KeyValConnectionWithExpire extends KeyValConnection {
  def expire(key: String, seconds: Int)
}