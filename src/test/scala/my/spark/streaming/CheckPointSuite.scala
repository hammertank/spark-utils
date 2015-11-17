package my.spark.streaming

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectStreamClass

import org.scalatest.FunSuite

import my.spark.streaming.statistics.StatTask
import my.spark.util.SerdeUtils

class CheckPointSuite extends FunSuite {

  class ObjectInputStreamWithLoader(inputStream_ : InputStream, loader: ClassLoader)
      extends ObjectInputStream(inputStream_) {

    override def resolveClass(desc: ObjectStreamClass): Class[_] = {
      try {
        return loader.loadClass(desc.getName())
      } catch {
        case e: Exception =>
      }
      super.resolveClass(desc)
    }
  }

  object DummyStatTask extends StatTask[Int, Int, Int] {
    override val valueField = null
    override def resolveValue(accuData: Int) = accuData

    override val recoverField = null
    override val initAccuData = 0

    override def runInternal(seq: Seq[Int], accuData: Int) = {
      0
    }
  }

  test("Object deserialize") {
    val a = List("a")
    
    println(a.getClass.getName)
    this.getClass.getClassLoader.loadClass(a.getClass.getName)
  }
}