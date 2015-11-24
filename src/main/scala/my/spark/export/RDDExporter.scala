package my.spark.export

import org.apache.spark.rdd.RDD
import my.spark.connection.ConnectionPool
import my.spark.connection.JdbcConnectionPool
import my.spark.connection.RedisConnectionPool
import redis.clients.jedis.Jedis
import java.sql.Connection

/**
 * @author hammertank
 *
 * Export RDD to different kinds of storage
 *
 */
object RDDExporter {

  /**
   * Export RDD to relational database
   *
   * @param insertSQL a function used to convert an instance of `U` to an insert SQL
   * @param needDelete whether to delete old data before export
   * @param deleteSQL a delete SQL which an be executed directly
   */
  def exportToRDB[U](dataRDD: RDD[U], insertSQL: U => String,
                     needDelete: Boolean = false, deleteSQL: String = "") {
    //Delete old data
    if (needDelete) {
      val connection = JdbcConnectionPool.borrowConnection
      connection.setAutoCommit(true)
      val stmt = connection.prepareStatement(deleteSQL)
      stmt.execute()
      stmt.close()
      JdbcConnectionPool.returnConnection(connection)
    }

    //Insert new data
    dataRDD.foreachPartition {
      partitionIter =>
        val connection = JdbcConnectionPool.borrowConnection
        connection.setAutoCommit(false)
        val stmt = connection.createStatement
        var sqls = Array[String]()
        partitionIter.foreach { row => sqls = sqls :+ insertSQL(row) }

        sqls.foreach { sql => stmt.addBatch(sql) }

        try {
          stmt.executeBatch()
          connection.commit()
        } catch {
          case ex: Exception =>
            connection.rollback()
            throw new Exception("Error SQLs: " + sqls.toSet.toString, ex)
        } finally {
          stmt.close
          JdbcConnectionPool.returnConnection(connection)
        }
    }
  }

  /**
   * Export RDD to Redis
   *
   * @param f a function which can store an instance of `U` into Redis with a `Jedis` object
   */
  def exportToRedis[U](rdd: RDD[U], f: (Jedis, U) => Unit) {
    exportByPartition(rdd, RedisConnectionPool, f)
  }

  def exportByPartition[U, C](rdd: RDD[U], pool: ConnectionPool[C], f: (C, U) => Unit) {
    rdd.foreachPartition(partitionIterator => {
      val connection = pool.borrowConnection
      try {
        partitionIterator.foreach(f(connection, _))
      } finally {
        pool.returnConnection(connection)
      }
    })
  }
}