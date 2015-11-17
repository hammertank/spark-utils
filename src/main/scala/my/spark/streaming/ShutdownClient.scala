package my.spark.streaming

import my.spark.socket.SimpleSocketClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.io.EOFException

object ShutdownClient {
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

    val (host, port) = ShutdownServer.shudownServerInfo(FileSystem.get(conf))
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