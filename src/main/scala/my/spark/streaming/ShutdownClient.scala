package my.spark.streaming

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.net.Socket
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import my.spark.util.ConfigUtils

object ShutdownClient {

  val checkpointDir = ConfigUtils.getString("application.checkpoint", null)

  if (checkpointDir == null) {
    throw new Exception("Property 'application.checkpoint' cannot be null")
  }

  def main(args: Array[String]) {

    var cmd = ""

    if (args.length < 1) {
      println(s"Usage: ${ShutdownClient.getClass.getName} [stop|kill]")
      sys.exit(1)
    } else {
      cmd = args(0).trim.toLowerCase()
      if (cmd != "stop" && cmd != "kill") {
        println(s"Invalid command: ${cmd}")
        sys.exit(1)
      }
    }

    val conf = new Configuration
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

    val (host, port) = ShutdownServer.readShudownServerInfo(checkpointDir, FileSystem.get(conf))
    val client = new SimpleSocketClient(host, port)

    println(s"Send command '${cmd}' to ${host}:${port}.")
    if (cmd == "stop")
      println("This may take some time. Please wait ...")
    else if (cmd == "kill")
      println("Command 'kill' may cause data loss. Use it only when server has no repsonse to 'stop'. ")

    try {
      client.send(cmd)
      println(client.receive)
    } catch {
      case e: EOFException =>
        println("Error: Connection is closed by server.")
    }

    client.close
  }
}

class SimpleSocketClient(host: String, port: Int) {

  val socket = new Socket(host, port)
  val dataInStream = new DataInputStream(socket.getInputStream)
  val dataOutStream = new DataOutputStream(socket.getOutputStream)

  def send(cmd: String) {
    dataOutStream.writeUTF(cmd)
    dataOutStream.flush()
  }

  def receive() = {
    dataInStream.readUTF()
  }

  def close {
    dataInStream.close()
    dataOutStream.close()
    socket.close()
  }
}