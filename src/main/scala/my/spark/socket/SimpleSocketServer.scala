package my.spark.socket

import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.ServerSocket
import java.net.Socket
import scala.actors.Actor
import java.net.SocketException

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