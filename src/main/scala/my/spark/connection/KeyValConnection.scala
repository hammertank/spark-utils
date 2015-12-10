package my.spark.connection

trait KeyValConnection {

  def get(key: Array[Byte], field: Array[Byte]): Array[Byte]
  def put(key: Array[Byte], field: Array[Byte], value: Array[Byte])
  def delete(key: Array[Byte])

  def close
}

trait KeyValConnectionWithExpire extends KeyValConnection {
  def expire(key: String, seconds: Int)
  def expire(key: Array[Byte], seconds: Int)
}