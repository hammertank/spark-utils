package my.spark.streaming.kafka

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import my.spark.streaming.ShutdownServer
import my.spark.util.ConfigUtils

trait KafkaApp extends Logging {
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
  val KAFKA_THREADS_PER_TOPIC = "kafka.threads.per.topic"

  val zkQuorum = ConfigUtils.getString(KAFKA_ZK_ADDRESS, "localhost:2181")
  val group = ConfigUtils.getString(KAFKA_GROUP_ID, "defalut")
  val topics = ConfigUtils.getString(KAFKA_TOPICS, "defalut")
  val numThreads = ConfigUtils.getInt(KAFKA_THREADS_PER_TOPIC, 1)

  val isDebug = ConfigUtils.getBoolean("application.debug", false)

  private var ssc_ : StreamingContext = null

  def ssc = ssc_

  def main(args: Array[String]) {
    ssc_ = StreamingContext.getOrCreate(checkPointDir, createContext)

    val shutdownServer = new ShutdownServer(shutdownPort, maxRetries, ssc)
    ShutdownServer.saveShutdownServerInfo(checkPointDir, shutdownServer)
    shutdownServer.start

    ssc.start()
    ssc.awaitTermination()
  }

  private def createContext(): StreamingContext = {
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))

    val topicMap = topics.split(",").map((_, numThreads)).toMap

    logInfo("Create Kafka Consumer with properteis: zookeeper=%s group=%s topics=%s".format(zkQuorum, group, topics))
    val recordDstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    compute(recordDstream)

    ssc.checkpoint(checkPointDir)
    ssc
  }

  //Subclasses need to override it 
  def compute(recordDStream: DStream[(String, String)])
}
