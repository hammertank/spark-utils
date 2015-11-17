package my.spark.streaming

import my.spark.streaming.statistics.StatTask

class DummyCheckPoint extends Serializable {
  val array = Array(DummyStatTask)
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