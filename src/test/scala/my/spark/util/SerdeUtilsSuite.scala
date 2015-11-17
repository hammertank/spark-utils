package my.spark.util

import org.scalatest.FunSuite
import scala.reflect.ClassTag

class SerdeUtilsSuite extends FunSuite {
  test("Test convertFromByteArray with null") {
    val obj = SerdeUtils.convertFromByteArray(null)
    assert(obj == null)
  }

  test("Test convertFromByteArray with invalid ByteArray") {
    val obj = SerdeUtils.convertFromByteArray(Array[Byte](1, 2, 3))
    assert(obj == null)
  }

  test("Test convertFromByteArray with 'null' ByteArray") {
    val obj = SerdeUtils.convertFromByteArray(SerdeUtils.convertToByteArray(null))
    assert(obj == null)
  }

  test("Test convertToByteArray with null") {
    val byteArray = SerdeUtils.convertToByteArray(null)
    assert(byteArray.isInstanceOf[Array[Byte]])
  }
}