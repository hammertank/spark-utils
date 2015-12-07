package my.spark.connection

trait KeyValConnection {

  def close

  def get(key: Array[Byte], field: Array[Byte]): Array[Byte]
  def put(key: Array[Byte], field: Array[Byte], value: Array[Byte])
}

trait KeyValConnectionWithExpire extends KeyValConnection {
  def expire(key: String, seconds: Int)
}