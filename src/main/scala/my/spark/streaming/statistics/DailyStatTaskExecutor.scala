package my.spark.streaming.statistics

import scala.collection.mutable.Set
import scala.reflect.ClassTag

import my.spark.connection.ConnectionPool
import my.spark.connection.KeyValConnection
import my.spark.util.DateUtils

/**
 * @author hammertank
 *
 * A StatTaskExecutor that will clean data both in dstream and external storage by key at the end  of the day
 *
 * External storage need to support operations like `Jedis.expire(key: String, seconds: Int)`
 *
 * @param <K>
 * @param <V>
 */
class DailyStatTaskExecutor[K, V](statTasks: List[_ <: Class[StatTask[V, _, _]]],
                                  resolveKey: V => K,
                                  keyForStore: K => String,
                                  pool: ConnectionPool[KeyValConnection])(implicit kt: ClassTag[K], vt: ClassTag[V])
    extends StatTaskExecutor[K, V](statTasks :+ classOf[DateRecorder[V]],
      resolveKey,
      keyForStore,
      pool.asInstanceOf[ConnectionPool[KeyValConnection]])(kt, vt) {

  override protected def accumulate(seq: Seq[V],
                                    taskSetOpt: Option[Set[StatTask[V, _, _]]]): Option[Set[StatTask[V, _, _]]] = {

    val currentDate = DateUtils.fromTimestamp(System.currentTimeMillis())
    val newTask = Set[StatTask[V, _, _]]()

    if (isDebug) {
      println("DailyStatTaskRunner.accumulate:")
    }

    taskSetOpt match {
      case Some(taskSet) =>
        val date = taskSet.filter(task => task.isInstanceOf[DateRecorder[V]])
          .head.data

        if (isDebug) {
          println("accDataOpt != None")
          println(s"date=$date currentDate=$currentDate seq.length=${seq.length}")
        }

        if (date == currentDate) { //In the same day, just accumulate data.
          if (isDebug) println("Accumulate...")
          super.accumulate(seq, taskSetOpt)
        } else if (seq.length != 0) { // Step into new day with new data. Reset `accuData`. Then accumulate.
          if (isDebug) println("Reset and Accumulate...")
          taskSet.foreach(task => task.reset())
          super.accumulate(seq, Some(newTask))
        } else { // Step into new day without new data. Eliminate the key-value pair
          if (isDebug) println("Eliminate...")
          None
        }
      case None =>
        if (isDebug) println("accDataOpt == None")
        super.accumulate(seq, taskSetOpt)
    }
  }
}

private[statistics] class DateRecorder[V](storeKey: String) extends StatTask[V, String, String](storeKey) {
  val valueField: Array[Byte] = null
  def resolveValue(accuData: String): String = null

  val recoverField: Array[Byte] = null
  val initAccuData: String = ""

  private var isDateChanged = true
  protected def runInternal(seq: Seq[V], accuData: String): String = {
    val current = DateUtils.fromTimestamp(System.currentTimeMillis())

    if (current != data) {
      isDateChanged = true;
    } else {
      isDateChanged = false
    }

    current
  }

  override def save(conn: KeyValConnection) {
    if (isDateChanged) {
      conn.expire(storeKey, DateUtils.secondsLeftToday())
    }
  }

  override def recover(conn: KeyValConnection) = {
    data = initAccuData
    this
  }
}