package my.spark.connection

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.FunSuite

class HBaseConnectionPoolSuite extends FunSuite {

  test("Put with Connection") {
    val time0 = System.currentTimeMillis()
    val conn = HBaseConnectionPool.borrowConnection()
    val time1 = System.currentTimeMillis() - time0
    println("Time1=" + time1 + " ms")
    conn.put(Bytes.toBytes("test"), Bytes.toBytes("pv"), Bytes.toBytes("1"))
    val time2 = System.currentTimeMillis() - time0
    println("Time2=" + time2 + " ms")
    HBaseConnectionPool.returnConnection(conn)
    val time3 = System.currentTimeMillis() - time0
    println("Time3=" + time3 + " ms")
  }

  test("Get with Connection") {
    val conn = HBaseConnectionPool.borrowConnection()
    val pv = new String(conn.get(Bytes.toBytes("test"), Bytes.toBytes("pv")), "utf-8")
    HBaseConnectionPool.returnConnection(conn)
  }

}