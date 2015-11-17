package my.spark.socket

import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket

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