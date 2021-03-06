package my.spark.connection

import java.sql.Connection
import com.alibaba.druid.pool.DruidDataSource
import com.typesafe.config.ConfigFactory

/**
 * @author hammertank
 *
 */
object JdbcConnectionPool extends ConnectionPool[Connection] {

  val JDBC_URL = "jdbc.url"
  val USERNAME = "jdbc.username"
  val PASSWD = "jdbc.passwd"
  val MAX_ACTIVE = "jdbc.max.active"
  val MIN_IDLE = "jdbc.min.idle"

  val config = ConfigFactory.load()

  private lazy val pool = {
    val dataSource = new DruidDataSource
    dataSource.setUrl(config.getString(JDBC_URL))
    dataSource.setUsername(config.getString(USERNAME))
    dataSource.setPassword(config.getString(PASSWD))
    dataSource.setMaxActive(config.getInt(MAX_ACTIVE))
    dataSource.setMinIdle(config.getInt(MIN_IDLE))

    dataSource
  }

  override def borrowConnection: Connection = {
    pool.getConnection
  }

  override def returnConnection(connection: Connection) {
    connection.close
  }

  sys.addShutdownHook {
    println("Execute hook thread: " + this)
    pool.close()
  }
}