package my.spark.util

import java.util.concurrent.TimeUnit
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * @author zhouyifan
 *
 */
object DateUtils {

  val SECONDS_IN_DAY = 24 * 3600

  val simpleDateFormatTL = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = {
      return new SimpleDateFormat
    }
  }

  val calendarTL = new ThreadLocal[Calendar]() {
    override def initialValue(): Calendar = {
      return Calendar.getInstance
    }
  }

  /**
   * Compute a time range
   *
   * @param dateStr a date string with given `pattern`
   * @param days number of days before `dateStr`(include `dateStr`)
   * @param pattern date pattern, default "yyyyMMdd"
   * @return a tuple containing starttime and endtime in unix timestamp
   */
  def getTimeRangeInUnixTimestamp(dateStr: String, days: Int, pattern: String = "yyyyMMdd") = {
    val dateFormatter = simpleDateFormatTL.get
    dateFormatter.applyPattern(pattern)
    val date = dateFormatter.parse(dateStr)
    val end = date.getTime / 1000 + SECONDS_IN_DAY
    val start = end - days * SECONDS_IN_DAY

    (start, end)
  }

  /**
   * return a date string 
   *
   * @param timestamp milliseconds since the epoch
   * @param pattern date pattern, default "yyyyMMdd"
   * @return date string with given `pattern`
   */
  def fromTimestamp(timestamp: Long, pattern: String = "yyyyMMdd") = {
    val calendar = calendarTL.get
    calendar.setTimeInMillis(timestamp)

    val dateFormatter = simpleDateFormatTL.get
    dateFormatter.applyPattern(pattern)
    dateFormatter.format(calendar.getTime)
  }

  /**
   * @return the rest seconds of today
   */
  def secondsLeftToday() = {
    val calendar = calendarTL.get
    val hour = calendar.get(Calendar.HOUR_OF_DAY)
    val min = calendar.get(Calendar.MINUTE)
    val seconds = calendar.get(Calendar.SECOND)

    SECONDS_IN_DAY - hour * 3600 - min * 60 - seconds
  }

}