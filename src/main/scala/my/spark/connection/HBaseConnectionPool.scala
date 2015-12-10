package my.spark.connection

import java.util.ArrayList

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Put

import my.spark.util.ConfigUtils

object HBaseConnectionPool extends ConnectionPool[KeyValConnection] {

  lazy val conf = HBaseConfiguration.create()

  private lazy val connection = HConnectionManager.createConnection(conf)

  val table: Array[Byte] = ConfigUtils.getString("hbase.table", null).getBytes("utf-8")
  val columnFamily: Array[Byte] = ConfigUtils.getString("hbase.columnfamily", null).getBytes("utf-8")
  val batchSize: Int = ConfigUtils.getInt("hbase.batchsize", 100)

  override def borrowConnection() = {
    new HBaseConnection(connection.getTable(table), columnFamily, batchSize)
  }

  override def returnConnection(conn: KeyValConnection) {
    conn.close
  }

  sys.addShutdownHook {
    println("Execute hook thread: " + this)
    connection.close
  }
}

class HBaseConnection(table: HTableInterface, columnFamily: Array[Byte], batchSize: Int) extends KeyValConnection {

  private var puts: java.util.List[Put] = new ArrayList[Put]()

  private def onPut() {
    if (puts.size() >= batchSize) {
      table.put(puts)
      puts = new ArrayList[Put]()
    }
  }

  override def put(key: Array[Byte], field: Array[Byte], value: Array[Byte]) {
    val put = new Put(key).add(columnFamily, field, value)
    puts.add(put)
    onPut()
  }

  override def get(key: Array[Byte], field: Array[Byte]): Array[Byte] = {
    val get = new Get(key).addColumn(columnFamily, field)
    table.get(get).getValue(columnFamily, field)
  }

  override def delete(key: Array[Byte]) {
    val delete = new Delete(key)
    table.delete(delete)
  }

  override def close() {
    if (puts.size() > 0) {
      table.put(puts)
    }

    table.close()
  }
}