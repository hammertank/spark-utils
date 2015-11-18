package my.spark.streaming.statistics

import my.spark.util.ConfigUtils
import my.spark.util.SerdeUtils

import redis.clients.jedis.Jedis

/**
 * @author hammertank
 *
 * A statistic task
 *
 * @param <A> Origin data type
 * @param <B> Middle result data type
 * @param <C> Output result data type
 */
abstract class StatTask[A, B, C] extends Serializable {

  val isDebug = ConfigUtils.getBoolean("application.debug", false) 
  
  val valueField: Array[Byte]
  def resolveValue(accuData: B): C

  val recoverField: Array[Byte]
  val initAccuData: B

  def run(seq: Seq[A], accuData: Any) = {
    runInternal(seq, cast(accuData))
  }

  private def cast(accuData: Any): B = {
    accuData.asInstanceOf[B]
  }

  /**
   * Aggregate `accuData` using new incoming data `seq`
   * 
   * @param seq incoming data
   * @param accuData old `accuData`
   * @return new `accuData`
   */
  protected def runInternal(seq: Seq[A], accuData: B): B

  /**
   * save data to Redis
   *
   * @param jedis a Redis connection
   * @param record data to save
   */
  def save(jedis: Jedis)(record: (String, Any)) {
    val redisKey = record._1.getBytes("utf-8")
    val value = resolveValue(cast(record._2)).toString().getBytes("utf-8")
    val recover = SerdeUtils.convertToByteArray(record._2)

    jedis.hset(redisKey, valueField, value)
    jedis.hset(redisKey, recoverField, recover)
  }

  /**
   * fetch data from Redis
   * This method is called to recover data
   * from an update of the application
   *
   * @param jedis a Redis connection
   * @param key key of data in Redis
   * @return data
   */
  def recover(jedis: Jedis, key: String): B = {
    val redisKey = key.getBytes("utf-8")

    val byteArray = jedis.hget(redisKey, recoverField)

    if (byteArray == null) {
      initAccuData
    } else {
      cast(SerdeUtils.convertFromByteArray(byteArray))
    }
  }

}
