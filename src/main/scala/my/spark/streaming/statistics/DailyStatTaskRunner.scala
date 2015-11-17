package my.spark.streaming.statistics

import scala.reflect.ClassTag
import scala.collection.mutable.Map
import redis.clients.jedis.Jedis
import my.spark.util.DateUtils

/**
 * @author zhouyifan
 *
 * @param <K>
 * @param <V>
 */
class DailyStatTaskRunner[K, V](statTasks: List[StatTask[V, _, _]],
  resolveKey: V => K, keyForRedis: K => String)(implicit kt: ClassTag[K], vt: ClassTag[V])
  extends StatTaskRunner[K, V](statTasks :+ new DateRecorder[V](), resolveKey, keyForRedis)(kt, vt) {

  override protected def accumulate(seq: Seq[V],
    accDataOpt: Option[Map[StatTask[V, _, _], Any]]): Option[Map[StatTask[V, _, _], Any]] = {

    val currentDate = DateUtils.fromTimestamp(System.currentTimeMillis())
    val newMap = Map[StatTask[V, _, _], Any]()

    if (isDebug) {
      println("DailyStatTaskRunner.accumulate:")
    }

    accDataOpt match {
      case Some(accMap) =>
        val date = accMap.filterKeys(key => key.isInstanceOf[DateRecorder[V]])
          .head._2.asInstanceOf[String]

        if (isDebug) {
          println("accDataOpt != None")
          println(s"date=$date currentDate=$currentDate seq.length=${seq.length}")
        }

        if (date == currentDate) { //In the same day, just accumulate data.
          if (isDebug) println("Accumulate...")
          super.accumulate(seq, accDataOpt)
        } else if (seq.length != 0) { // Step into new day with new data. Reset `accuData`. Then accumulate.
          if (isDebug) println("Reset and Accumulate...")
          accMap.keys.foreach(key => newMap(key) = key.initAccuData)
          super.accumulate(seq, Some(newMap))
        } else { // Step into new day without new data. Eliminate the key-value pair
          if (isDebug) println("Eliminate...")
          None
        }
      case None =>
        if (isDebug) println("accDataOpt == None")
        super.accumulate(seq, accDataOpt)
    }
  }

  override protected def save(jedis: Jedis)(stat: (K, Map[StatTask[V, _, _], Any])) {
    super.save(jedis)(stat)

    jedis.expire(keyForRedis(stat._1), DateUtils.secondsLeftToday())
  }

}

class DateRecorder[V] extends StatTask[V, String, Any] {
  val valueField: Array[Byte] = null
  def resolveValue(accuData: String): Any = null

  val recoverField: Array[Byte] = null
  val initAccuData: String = ""

  protected def runInternal(seq: Seq[V], accuData: String): String = {
    DateUtils.fromTimestamp(System.currentTimeMillis())
  }

  override def save(jedis: Jedis)(record: (String, Any)) {}

  override def recover(jedis: Jedis)(key: String) = initAccuData
}