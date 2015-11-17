package my.spark.import_

import java.sql.ResultSet
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.rdd.RDD
import my.spark.util.ConnectionPool
import org.apache.spark.rdd.EmptyRDD
import java.sql.Statement

/**
 * @author zhouyifan
 *
 */
object Importer {

  /**
   *
   * Import data from relational database
   *
   * @param selectSQL select SQL
   * @param convertFunc function used to convert one Row to target instance of `U`
   * @return instance of `U` in Array
   */
  def importFromRDB[U](selectSQL: String, convertFunc: (ResultSet) => U)(implicit tag: ClassTag[U]): Array[U] = {
    var array = Array[U]()
    val func = (stmt: Statement) => {
      val resultSet = stmt.executeQuery(selectSQL)
      while (resultSet.next()) {
        array = array :+ convertFunc(resultSet)
      }
    }
    executeAction(func)
    array
  }

  /**
   * Import data from database as a RDD
   *
   * It calls `org.apache.spark.rdd.JdbcRDD` internal
   *
   * @param sc SparkContext
   * @param keySelectSQL SQL used to select keys of data
   * @param partNum the number of partitions. Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2, the query would be executed twice, once with (1, 10) and once with (11, 20)
   * @param selectSQL SQL used to select data. SQL must contain two ? placeholders for parameters used to partition the results. E.g. "select title, author from books where ? <= id and id <= ?"
   * @param convertFunc a function from a ResultSet to a single row of the desired result type(s).
   * @return a RDD contains instances of `U`
   */
  def importFromRDB[U](sc: SparkContext, keySelectSQL: String, partNum: Int,
    selectSQL: String, convertFunc: (ResultSet) => U)(implicit tag: ClassTag[U]): RDD[U] = {
    val keys = importFromRDB(keySelectSQL, (rSet: ResultSet) => { rSet.getLong(1) })

    if (keys.size == 0) {
      sc.emptyRDD
    } else {
      new JdbcRDD(sc, () => ConnectionPool.borrowConnection, selectSQL, keys.min, keys.max, partNum, convertFunc)
    }
  }

  /**
   * Import one row from database
   *
   * @param selectSQL SQL used to select keys of data
   * @param convertFunc a function from a ResultSet to a single row of the desired result type(s).
   * @return an instance of `U`
   */
  def importOneFromRDB[U](selectSQL: String, convertFunc: (ResultSet) => U)(implicit tag: ClassTag[U]): U = {
    var ret: U = null.asInstanceOf[U]
    val func = (stmt: Statement) => {
      val resultSet = stmt.executeQuery(selectSQL)
      if (resultSet.next)
        ret = convertFunc(resultSet)
    }
    executeAction(func)
    ret
  }

  private def executeAction(func: (Statement) => _) {
    val connection = ConnectionPool.borrowConnection
    val stmt = connection.createStatement()
    func(stmt)
    stmt.close()
    ConnectionPool.returnConnection(connection)
  }

}