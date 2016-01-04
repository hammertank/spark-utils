package my.spark.connection

import java.io.BufferedWriter
import java.io.FileWriter
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.FunSuite

import my.spark.util.DateUtils

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

  ignore("Get performance") {
    val baseRoomId = 15390390

    val concurrency = 100000
    //    val userNum = userids.size
    val bw = new BufferedWriter(new FileWriter("target/concurrent-test.csv"))

    var slowCount = new AtomicLong(0)
    var count = new AtomicLong(0)

    val conn = HBaseConnectionPool.borrowConnection()
    conn.get("123".getBytes, "123".getBytes)
    val fieldBytes = "pv".getBytes("utf-8")

    sys.addShutdownHook {
      bw.flush()
      bw.close()
    }

    println(s"Test RoomRcmdServer with Concurrency: $concurrency")
    while (true) {
      for (i <- (1 to concurrency)) {

        val key = "room_stat:" + (baseRoomId + Random.nextInt(10000)) + ":0:" + "2015-12-11"
        val ret: Future[Unit] = Future {
          val starttime = System.currentTimeMillis()
          try {
            val response = conn.get(key.getBytes("utf-8"), fieldBytes)
          } catch {
            case _: Exception =>
          }
          val endtime = System.currentTimeMillis()
          val duration = endtime - starttime
          val c = count.incrementAndGet()
          bw.write(s"$key,$duration,${DateUtils.fromTimestamp(starttime, "yyyy-MM-dd HH:mm:ss")},${DateUtils.fromTimestamp(endtime, "yyyy-MM-dd HH:mm:ss")}")
          bw.newLine()
          if (c % 100 == 0) { bw.flush }
          //          println(s"$name - recommend count $count")
          //          println(s"$name - recommend for user ${userids(index)} in ${endtime - starttime} ms")
          if (duration > 1000) {
            val c = slowCount.incrementAndGet()
            println(s"Slow response count: $c")
          }
        }
      }
      Thread.sleep(1000)
    }
  }

}