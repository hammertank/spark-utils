package my.spark.streaming.statistics

import scala.collection.mutable.Map
import scala.reflect.ClassTag

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import my.spark.export.RDDExporter
import my.spark.util.ConfigUtils
import my.spark.util.RedisConnectionPool

import redis.clients.jedis.Jedis

/**
 * @author zhouyifan
 *
 * Run statistic tasks and save task results to Redis
 *
 * @param <K> record key type
 * @param <V> record value type
 * @param statTasks tasks need to run
 * @param resolveKey func used to resolve record key from record value
 * @param keyForRedis func used to convert key to key in Redis
 */
class StatTaskRunner[K, V](statTasks: List[StatTask[V, _, _]],
  resolveKey: V => K, keyForRedis: K => String)(implicit kt: ClassTag[K], vt: ClassTag[V]) extends Serializable {

  val isDebug = ConfigUtils.getBoolean("application.debug", false)

  /**
   * Extract statistics info from  `dstream`
   *
   * @param dstream data source
   */
  def run(dstream: DStream[V]) {

    if (isDebug) {
      println("StatTaskRunner.run:")
      statTasks.foreach(s => println(s.getClass.getName))
    }

    dstream.map {
      v => (resolveKey(v), v)
    }.updateStateByKey(accumulate).foreachRDD(RDDExporter.exportToRedis(_, save))
  }

  /**
   * Accumulate data with `StatTask`s
   *
   * This method is a parameter of `PairDStreamFunctions.updateStateByKey`
   *
   * @param seq new incoming data
   * @param accDataOpt A Map whose keys are `StatTask`s and the values are the data to be processed by its key
   * @return A new Map updated by `StatTask`s
   */
  protected def accumulate(seq: Seq[V],
    accDataOpt: Option[Map[StatTask[V, _, _], Any]]): Option[Map[StatTask[V, _, _], Any]] = {

    if (isDebug) {
      println("StatTaskRunner.accumulate:")

      if (!accDataOpt.isEmpty)
        println("Map hash code: " + accDataOpt.get.hashCode())

      if (seq.length != 0) {
        println("Key: " + resolveKey(seq.head))
      }
    }

    val newMap = Map[StatTask[V, _, _], Any]()

    accDataOpt match {
      case Some(accMap) =>
        for (entry <- accMap) {
          val task = entry._1
          val data = entry._2
          newMap(task) = task.run(seq, data)
        }

        Some(newMap)
      case None =>
        if (seq.length == 0) {
          None
        } else {
          val key = resolveKey(seq.head)
          val redisKey = keyForRedis(key)
          val jedis = RedisConnectionPool.borrowConnection()
          for (task <- statTasks) {
            val recover = task.recover(jedis)(redisKey)
            newMap.put(task, task.run(seq, recover))
          }
          RedisConnectionPool.returnConnection(jedis)
          Some(newMap)
        }
    }
  }

  /**
   * Save data to redis
   *
   * @param jedis a Redis connection
   * @param stat data to be saved
   */
  protected def save(jedis: Jedis)(stat: (K, Map[StatTask[V, _, _], Any])) {

    stat._2.foreach {
      case (key, value) => {
        key.save(jedis)(keyForRedis(stat._1), value)
      }
    }
  }
}