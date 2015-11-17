package my.spark.socket

import org.scalatest.Finders
import org.scalatest.FunSuite
import java.net.Socket
import java.io.DataOutputStream
import java.io.DataInputStream

class SimpleSocketSuite extends FunSuite {

  test("test Server and Client") {

    def func(s: Socket) = {
      val dataInStream = new DataInputStream(s.getInputStream)
      val dataOutStream = new DataOutputStream(s.getOutputStream)
      dataOutStream.writeUTF(dataInStream.readUTF())
      dataOutStream.close
      true
    }

    val server = new SimpleSocketServer(func, 7788)

    val client = new SimpleSocketClient("localhost", 7788)

    server.start()

    val cmd = "cmd"
    client.send(cmd)
    
    val response = client.receive()

    assert(response == cmd)
    
    client.close
  }
}