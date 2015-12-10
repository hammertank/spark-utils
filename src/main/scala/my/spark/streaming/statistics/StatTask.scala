package my.spark.streaming.statistics

import my.spark.util.ConfigUtils
import my.spark.util.SerdeUtils
import my.spark.connection.KeyValConnection

/**
 * @author hammertank
 *
 * A statistic task
 *
 * @param <A> Origin data type
 * @param <B> Middle result data type
 * @param <C> Output result data type
 */
abstract class StatTask[A, B, C](key: String) extends Serializable {

  val keyBytes = key.getBytes("utf-8");

  val isDebug = ConfigUtils.getBoolean("application.debug", false)

  val valueField: Array[Byte]
  def resolveValue(accuData: B): C

  val recoverField: Array[Byte]
  val initAccuData: B

  private[statistics] var data: B = _
  private[statistics] var isDataChanged: Boolean = true

  def run(seq: Seq[A]) {
    if (seq.length > 0) {
      data = runInternal(seq, data)
      isDataChanged = true
    } else {
      isDataChanged = false
    }
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
   * save data to external storage
   *
   * @param conn a external storage connection
   * @param record data to save
   */
  def save(conn: KeyValConnection) {
    if (isDataChanged) {
      val value = resolveValue(data).toString().getBytes("utf-8")
      val recover = SerdeUtils.convertToByteArray(data)

      conn.put(keyBytes, valueField, value)
      conn.put(keyBytes, recoverField, recover)
    }
  }

  /**
   * fetch data from external storage
   * This method is called to recover data of `StatTask`
   *
   * @param conn a external storage connection
   * @param key key of data in external storage
   * @return `StatTask` itself
   */
  def recover(conn: KeyValConnection): this.type = {
    val byteArray = conn.get(keyBytes, recoverField)

    if (byteArray == null) {
      data = initAccuData
    } else {
      data = cast(SerdeUtils.convertFromByteArray(byteArray))
    }

    this
  }
}
