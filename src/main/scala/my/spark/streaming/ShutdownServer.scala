package my.spark.streaming

import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.BindException
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException

import scala.actors.Actor

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import my.spark.util.ConfigUtils
import my.spark.util.DFSUtils

class ShutdownServer(startPort: Int, maxRetries: Int, val ssc: StreamingContext) extends Logging {

  private var port = startPort

  val shutdownFunc = (socket: Socket) => {
    val dataInStream = new DataInputStream(socket.getInputStream)
    val dataOutStream = new DataOutputStream(socket.getOutputStream)
    val cmd = dataInStream.readUTF()

    val (ret, response) = try {
      if (cmd == "stop") {
        logInfo("Received signal 'stop'. Start to stop streaming context gracefully.")
        ssc.stop(true, true)
        (true, "Shutdown complete.")
      } else if (cmd == "kill") {
        logInfo("Received signal 'kill'. Start to stop sparkContext without stopping streaming context. Unprocessed data will be lost.")
        ssc.sparkContext.stop
        (true, "Shutdown complete.")
      } else {
        (false, "Invalid command: $cmd.")
      }
    } catch {
      case e: Exception =>
        logError("Shutdown with exception", e)
        (true, "Shutdown complete with errors.")
    }

    dataOutStream.writeUTF(response)
    dataOutStream.close

    if (cmd == "kill") {
      sys.exit // Force master container to exit
    }

    ret
  }

  val serverSocket = createServer

  private def createServer: SimpleSocketServer = {
    for (offset <- 0 to maxRetries) {
      try {
        // Do not try a privilege port
        port = ((startPort + offset - 1024) % (65536 - 1024)) + 1024
        val server = new SimpleSocketServer(shutdownFunc, port)
        logInfo(s"Successfully started ShutdownServer on port $port.")
        return server
      } catch {
        case ex: BindException if isBindCollision(ex) =>
          if (offset >= maxRetries) {
            val exceptionMessage = s"${ex.getMessage}: ShutdownServer failed after $maxRetries retries!"
            val exception = new BindException(exceptionMessage)
            exception.setStackTrace(ex.getStackTrace)
            throw exception
          }

          logWarning(s"ShutdownServer could not bind on port $port. " +
            s"Attempting port ${port + 1}.")
      }
    }

    // Should never happen
    throw new Exception(s"Failed to create ShutdownServer on port $port")
  }

  def start {
    serverSocket.start
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   *
   * From org.apache.spark.util.Utils
   */
  private def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _            => false
    }
  }
}

object ShutdownServer {

  def serverInfoFile(dir: String) = {
    new Path(dir, "server_info").toString
  }

  def saveShutdownServerInfo(dir: String, server: ShutdownServer, fs: FileSystem = FileSystem.get(new Configuration)) {
    val host = server.ssc.sparkContext.getConf.get("spark.driver.host")
    val port = server.port
    DFSUtils.save(serverInfoFile(dir), host + ":" + port)
  }

  def readShudownServerInfo(dir: String, fs: FileSystem = FileSystem.get(new Configuration)) = {
    val serverInfoStr = DFSUtils.read(serverInfoFile(dir), 1, fs)(0)
    val Array(hostStr, portStr) = serverInfoStr.split(":")
    (hostStr, portStr.toInt)
  }

}

class SimpleSocketServer(func: Socket => Boolean, port: Int) {

  val serverSocket = new ServerSocket(port)

  var started = false

  def handle(socket: Socket) = func(socket)

  def start() = synchronized {
    if (!started) {
      Actor.actor {
        @volatile var stop = false
        while (!stop) {
          try {
            val socket = serverSocket.accept()

            Actor.actor {
              try {
                val ret = handle(socket)
                if (ret) {
                  stop = ret
                }
              } finally {
                socket.close
              }
            }
          } catch {
            case e: SocketException =>
          }
        }

        serverSocket.close()
      }

      started = true
    }
  }

  sys.addShutdownHook({
    if (serverSocket != null) serverSocket.close
  })
}

object ShutdownServerTest {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(new SparkConf, Seconds(10))

    val host = ssc.sparkContext.getConf.get("spark.driver.host")
    val lines = ssc.socketTextStream(host, 9999)
    lines.foreachRDD(rdd => rdd.foreach(println))

    val shutdownServer = new ShutdownServer(7788, 10, ssc)
    shutdownServer.start

    ssc.start
    ssc.awaitTermination
  }
}