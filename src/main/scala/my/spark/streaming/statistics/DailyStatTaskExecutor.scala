package my.spark.streaming.statistics

import scala.collection.mutable.Set
import scala.reflect.ClassTag
import my.spark.connection.ConnectionPool
import my.spark.connection.KeyValConnection
import my.spark.util.DateUtils
import my.spark.connection.KeyValConnectionWithExpire

/**
 * @author hammertank
 *
 * A StatTaskExecutor that will renew `StatTask`s in dstream at the beginning  of a day.
 *
 * If external storage supports operations like `Jedis.expire(key: String, seconds: Int)`,
 * implement trait `KeyValConnectionWithExpire` and use it here to let records older than
 * one day expire in external storage.
 *
 * @param <K>
 * @param <V>
 */
class DailyStatTaskExecutor[V](statTasks: List[Class[_ <: StatTask[V, _, _]]],
                               resolveKey: V => String,
                               pool: ConnectionPool[KeyValConnection])(implicit vt: ClassTag[V])
    extends StatTaskExecutor[V](statTasks :+ classOf[DateRecorder[V]], resolveKey, pool)(vt) {

  override protected def accumulate(seq: Seq[V],
                                    taskListOpt: Option[List[StatTask[V, _, _]]]): Option[List[StatTask[V, _, _]]] = {

    val currentDate = DateUtils.fromTimestamp(System.currentTimeMillis())

    if (isDebug) {
      println("DailyStatTaskExecutor.accumulate:")
    }

    taskListOpt match {
      case Some(tasList) =>
        val date = tasList.filter(task => task.isInstanceOf[DateRecorder[V]])
          .head.data

        if (isDebug) {
          println("accDataOpt != None")
          println(s"date=$date currentDate=$currentDate seq.length=${seq.length}")
        }

        if (date == currentDate) { //In the same day, just accumulate data.
          if (isDebug) println("Accumulate...")
          super.accumulate(seq, taskListOpt)
        } else if (seq.length != 0) { // Step into new day with new data. Clean old `StatTask`s. Then accumulate.
          if (isDebug) println("Clean and Accumulate...")
          super.accumulate(seq, None)
        } else { // Step into new day without new data. Eliminate the key-value pair
          if (isDebug) println("Eliminate...")
          None
        }
      case None =>
        if (isDebug) println("accDataOpt == None")
        super.accumulate(seq, taskListOpt)
    }
  }
}

/**
 * @author hammertank
 *
 * Record the execution date and set the expire time of `key`.
 * `key` must expire at the end of a day.
 *
 * @param <V>
 */
private[statistics] class DateRecorder[V](key: String) extends StatTask[V, String, String](key) {
  val valueField: Array[Byte] = null
  def resolveValue(accuData: String): String = null

  val recoverField: Array[Byte] = null
  val initAccuData: String = ""
  //Fields above have no use in this class.

  private var expireIsSet = false

  // `data` represents the date this object created
  data = DateUtils.fromTimestamp(System.currentTimeMillis())

  // Do nothing
  override def runInternal(seq: Seq[V], accuData: String) = data

  /**
   * Set expire time of `key`.
   *
   * Only need to do once.
   *
   */
  override def save(conn: KeyValConnection) {
    if (!expireIsSet && conn.isInstanceOf[KeyValConnectionWithExpire]) {
      conn.asInstanceOf[KeyValConnectionWithExpire].expire(key, DateUtils.secondsLeftToday())
      expireIsSet = true
    }
  }

  // Override to avoid read from external storage
  override def recover(conn: KeyValConnection) = {
    this
  }
}