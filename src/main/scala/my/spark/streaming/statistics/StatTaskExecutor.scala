package my.spark.streaming.statistics

import scala.collection.mutable.Set
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
class StatTaskExecutor[K, V](taskClasses: List[Class[_ <: StatTask[V, _, _]]],
                             resolveKey: V => K,
                             keyForStore: K => String,
                             connectionPool: ConnectionPool[KeyValConnection])(implicit kt: ClassTag[K], vt: ClassTag[V]) extends Serializable {

  val isDebug = ConfigUtils.getBoolean("application.debug", false)

  /**
   * Extract statistics info from  `dstream` and export to external storage
   *
   * @param dstream data source
   */
  def run(dstream: DStream[V]) {

    if (isDebug) {
      println("StatTaskRunner.run:")
      taskClasses.foreach(s => println(s.getName))
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
   * @param accDataOpt Set of `StatTask`s
   * @return A new Set of `StatTask`s
   */
  protected def accumulate(seq: Seq[V],
                           taskSetOpt: Option[Set[StatTask[V, _, _]]]): Option[Set[StatTask[V, _, _]]] = {

    if (isDebug) {
      println("StatTaskRunner.accumulate:")

      if (!taskSetOpt.isEmpty)
        println("Map hash code: " + taskSetOpt.get.hashCode())

      if (seq.length != 0) {
        println("Key: " + resolveKey(seq.head))
      }
    }

    val newSet = Set[StatTask[V, _, _]]()

    taskSetOpt match {
      case Some(taskSet) =>
        for (task <- taskSet) {
          task.run(seq)
        }

        Some(taskSet)
      case None =>
        if (seq.length == 0) {
          None
        } else {
          val key = resolveKey(seq.head)
          val storeKey = keyForStore(key)
          val conn = connectionPool.borrowConnection()
          for (taskClass <- taskClasses) {
            val constructor = taskClass.getDeclaredConstructor(classOf[String])
            val task = constructor.newInstance(storeKey)
            task.recover(conn).run(seq)
            newSet += task
          }
          connectionPool.returnConnection(conn)
          Some(newSet)
        }
    }
  }

  /**
   * Save data to external storage
   *
   * @param conn a external storage connection
   * @param stat data to be saved
   */
  protected def save(conn: KeyValConnection, stat: (K, Set[StatTask[V, _, _]])) {
    stat._2.foreach {
      task => task.save(conn)
    }
  }
}