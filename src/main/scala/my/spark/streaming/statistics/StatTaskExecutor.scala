package my.spark.streaming.statistics

import scala.collection.mutable.Map
import scala.reflect.ClassTag

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import my.spark.connection.ConnectionPool
import my.spark.connection.KeyValConnection
import my.spark.export.RDDExporter
import my.spark.util.ConfigUtils

/**
 * @author hammertank
 *
 * Run statistic tasks and save task results to external storage
 *
 * @param <K> record key type
 * @param <V> record value type
 * @param statTasks tasks need to run
 * @param resolveKey func used to resolve record key from record value
 * @param keyForStore func used to convert key to key in external storage
 * @param connectionPool connection pool to get connection from
 */
class StatTaskExecutor[K, V](statTasks: List[StatTask[V, _, _]],
                             resolveKey: V => K,
                             keyForStore: K => String,
                             connectionPool: ConnectionPool[KeyValConnection])(implicit kt: ClassTag[K], vt: ClassTag[V])
    extends Serializable {

  val isDebug = ConfigUtils.getBoolean("application.debug", false)

  /**
   * Extract statistics info from  `dstream` and export to external storage
   *
   * @param dstream data source
   */
  def execute(dstream: DStream[V]) {

    if (isDebug) {
      println("StatTaskExecutor.run:")
      statTasks.foreach(s => println(s.getClass.getName))
    }

    dstream.map {
      v => (resolveKey(v), v)
    }.updateStateByKey(accumulate).foreachRDD(RDDExporter.exportByPartition(_, connectionPool, save))
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
          val storeKey = keyForStore(key)
          val conn = connectionPool.borrowConnection()
          for (task <- statTasks) {
            val recover = task.recover(conn, storeKey)
            newMap.put(task, task.run(seq, recover))
          }
          connectionPool.returnConnection(conn)
          Some(newMap)
        }
    }
  }

  /**
   * Save data to external storage
   *
   * @param conn a external storage connection
   * @param stat data to be saved
   */
  protected def save(conn: KeyValConnection, stat: (K, Map[StatTask[V, _, _], Any])) {

    stat._2.foreach {
      case (key, value) => {
        key.save(conn, (keyForStore(stat._1), value))
      }
    }
  }
}