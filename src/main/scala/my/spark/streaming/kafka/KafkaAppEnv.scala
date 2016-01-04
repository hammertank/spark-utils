package my.spark.streaming.kafka

import org.I0Itec.zkclient.ZkClient
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext

import my.spark.util.ConfigUtils

object KafkaAppEnv extends Logging {
  val BATCH_DURATION = "application.batch.duration"
  val CHECKPOINT = "application.checkpoint"
  val WINDOW_DURATION = "application.window.duration"
  val SLIDE_DURATION = "application.slide.duration"
  val SHUTDOWN_PORT = "application.shutdown.port"
  val MAX_RETRIES = "application.shutdown.port.maxRetries"

  val defaultName = getClass.getSimpleName

  val batchDuration = ConfigUtils.getInt(BATCH_DURATION, 10)
  val checkPointDir = ConfigUtils.getString(CHECKPOINT, defaultName)
  val windowDuration = ConfigUtils.getInt(WINDOW_DURATION, 20)
  val slideDuration = ConfigUtils.getInt(SLIDE_DURATION, 10)
  val shutdownPort = ConfigUtils.getInt(SHUTDOWN_PORT, 7788)
  val maxRetries = ConfigUtils.getInt(MAX_RETRIES, 10)

  val KAFKA_ZK_ADDRESS = "kafka.zk.address"
  val KAFKA_GROUP_ID = "kafka.group.id"
  val KAFKA_TOPICS = "kafka.topics"
  val KAFKA_BROKER_LIST = "kafka.metadata.broker.list"
  //  val KAFKA_THREADS_PER_TOPIC = "kafka.threads.per.topic"
  //
  val zkQuorum = ConfigUtils.getString(KAFKA_ZK_ADDRESS, "localhost:2181")
  val groupId = ConfigUtils.getString(KAFKA_GROUP_ID, "defalut")
  //  val numThreads = ConfigUtils.getInt(KAFKA_THREADS_PER_TOPIC, 1)

  val brokerList = ConfigUtils.getString(KAFKA_BROKER_LIST, "localhost:2181")
  val topics = ConfigUtils.getString(KAFKA_TOPICS, "defalut").split(",").map { x => x.trim() }.toSet

  val isDebug = ConfigUtils.getBoolean("application.debug", false)
  val zkClient: ZkClient = ZKUtils.createZKClient(zkQuorum)

  private var ssc_ : StreamingContext = null

  def setActiveContext(ssc: StreamingContext) {
    ssc_ = ssc
  }

  def ssc = ssc_

  sys.addShutdownHook {
    logInfo("close zkClient")
    zkClient.close()
  }
}