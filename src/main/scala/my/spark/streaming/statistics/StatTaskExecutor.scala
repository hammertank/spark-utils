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
 * @param connectionPool connection pool to get connection from
 */
class StatTaskExecutor[V](taskClasses: List[Class[_ <: StatTask[V, _, _]]],
                          resolveKey: V => String,
                          connectionPool: ConnectionPool[KeyValConnection])(implicit vt: ClassTag[V]) extends Serializable {

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
   * @param accDataOpt List of `StatTask`s
   * @return A new List of `StatTask`s
   */
  protected def accumulate(seq: Seq[V],
                           taskListOpt: Option[List[StatTask[V, _, _]]]): Option[List[StatTask[V, _, _]]] = {

    if (isDebug) {
      println("StatTaskRunner.accumulate:")

      if (!taskListOpt.isEmpty)
        println("Map hash code: " + taskListOpt.get.hashCode())

      if (seq.length != 0) {
        println("Key: " + resolveKey(seq.head))
      }
    }

    taskListOpt match {
      case Some(taskList) =>
        for (task <- taskList) {
          task.run(seq)
        }

        Some(taskList)
      case None =>
        if (seq.length == 0) {
          None
        } else {
          val key = resolveKey(seq.head)
          val conn = connectionPool.borrowConnection()

          val newList = taskClasses.map { taskClass =>
            val constructor = taskClass.getDeclaredConstructor(classOf[String])
            val task = constructor.newInstance(key)
            task.recover(conn).run(seq)
            task
          }

          connectionPool.returnConnection(conn)
          Some(newList)
        }
    }
  }

  /**
   * Save data to external storage
   *
   * @param conn a external storage connection
   * @param stat data to be saved
   */
  protected def save(conn: KeyValConnection, stat: (String, List[StatTask[V, _, _]])) {
    stat._2.foreach {
      task => task.save(conn)
    }
  }
}