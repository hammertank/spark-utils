package my.spark.streaming.kafka

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
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
  //  val KAFKA_TOPICS = "kafka.topics"
  //  val KAFKA_THREADS_PER_TOPIC = "kafka.threads.per.topic"
  //
  //  val zkQuorum = ConfigUtils.getString(KAFKA_ZK_ADDRESS, "localhost:2181")
  //  val groupId = ConfigUtils.getString(KAFKA_GROUP_ID, "defalut")
  //  val numThreads = ConfigUtils.getInt(KAFKA_THREADS_PER_TOPIC, 1)

  val KAFKA_BROKER_LIST = "kafka.metadata.broker.list"
  val KAFKA_TOPICS = "kafka.topics"

  val brokerList = ConfigUtils.getString(KAFKA_BROKER_LIST, "localhost:2181")
  val topics = ConfigUtils.getString(KAFKA_TOPICS, "defalut").split(",").map { x => x.trim() }.toSet

  val isDebug = ConfigUtils.getBoolean("application.debug", false)

  private var ssc_ : StreamingContext = null

  def ssc = ssc_

  def main(args: Array[String]) {
    ssc_ = StreamingContext.getOrCreate(checkPointDir, createContext)

    val shutdownServer = new ShutdownServer(shutdownPort, maxRetries, ssc)
    shutdownServer.start

    ssc.start()
    ssc.awaitTermination()
  }

  private def createContext(): StreamingContext = {
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))

    //    val topicMap = topics.split(",").map((_, numThreads)).toMap
    //
    //    logInfo("Create Kafka Consumer with properteis: zookeeper=%s group=%s topics=%s".format(zkQuorum, group, topics))
    //    val recordDstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)

    val zkQuorum = ConfigUtils.getString(KAFKA_ZK_ADDRESS, "localhost:2181")
    val groupId = ConfigUtils.getString(KAFKA_GROUP_ID, "defalut")
    val zkClient = ZKUtils.createZKClient(zkQuorum)
    val fromOffsets = ZKUtils.fetchLastOffsets(zkClient, groupId)

    logInfo(s"Create DirectKafkaInputDStream with properties: $KAFKA_ZK_ADDRESS=$zkQuorum $KAFKA_GROUP_ID=$groupId $KAFKA_BROKER_LIST=$brokerList $KAFKA_TOPICS=$topics")
    val recordDstream = if (fromOffsets.isDefined) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets.get, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    compute(recordDstream)

    recordDstream.foreachRDD { rdd =>
      val zkClient = ZKUtils.createZKClient(zkQuorum)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      ZKUtils.updateLastOffsets(zkClient, groupId, offsetRanges)
    }

    ssc.checkpoint(checkPointDir)
    ssc
  }

  //Subclasses need to override it 
  def compute(recordDStream: DStream[(String, String)])
}

object KafkaAppTest extends KafkaApp {

  override def compute(dstream: DStream[(String, String)]) {
    dstream.foreachRDD(rdd => rdd.foreach(println))
  }
}