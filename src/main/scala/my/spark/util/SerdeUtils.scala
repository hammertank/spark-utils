package my.spark.util

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

/**
 * @author hammertank
 *
 */
object SerdeUtils {

  def convertToByteArray(obj: Any): Array[Byte] = {
    val bops = new ByteArrayOutputStream
    val oops = new ObjectOutputStream(bops)
    oops.writeObject(obj)
    val res = bops.toByteArray()
    oops.close()
    bops.close()
    res
  }

  def convertFromByteArray(byteArray: Array[Byte]): Any = {
    if (byteArray == null || byteArray.length == 0) {
      null
    } else {
      try {
        val bips = new ByteArrayInputStream(byteArray)
        val oips = new ObjectInputStream(bips)
        val obj = oips.readObject()
        bips.close()
        oips.close()
        obj
      } catch {
        case ex: EOFException => null
      }
    }
  }
}