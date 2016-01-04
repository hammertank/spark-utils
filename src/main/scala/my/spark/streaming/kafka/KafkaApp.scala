package my.spark.streaming.kafka

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import my.spark.streaming.ShutdownServer

trait KafkaApp extends Logging {

  def main(args: Array[String]) {
    KafkaAppEnv.setActiveContext(StreamingContext.getOrCreate(KafkaAppEnv.checkPointDir, createContext))

    val shutdownServer = new ShutdownServer(KafkaAppEnv.shutdownPort, KafkaAppEnv.maxRetries, KafkaAppEnv.ssc)
    shutdownServer.start

    KafkaAppEnv.ssc.start()
    KafkaAppEnv.ssc.awaitTermination()
  }

  private def createContext(): StreamingContext = {
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(KafkaAppEnv.batchDuration))

    //    val topicMap = topics.split(",").map((_, numThreads)).toMap
    //
    //    logInfo("Create Kafka Consumer with properteis: zookeeper=%s group=%s topics=%s".format(zkQuorum, group, topics))
    //    val recordDstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> KafkaAppEnv.brokerList)

    val fromOffsets = ZKUtils.fetchLastOffsets(KafkaAppEnv.zkClient, KafkaAppEnv.groupId)

    logInfo(s"Create DirectKafkaInputDStream with properties: ${KafkaAppEnv.KAFKA_ZK_ADDRESS}=${KafkaAppEnv.zkQuorum} ${KafkaAppEnv.KAFKA_GROUP_ID}=${KafkaAppEnv.groupId} ${KafkaAppEnv.KAFKA_BROKER_LIST}=${KafkaAppEnv.brokerList} ${KafkaAppEnv.KAFKA_TOPICS}=${KafkaAppEnv.topics}")
    val recordDstream = if (fromOffsets.isDefined) {
      logInfo("Get fromOffsets from Zookeeper: " + fromOffsets.get)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets.get, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, KafkaAppEnv.topics)
    }

    compute(recordDstream)

    recordDstream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      ZKUtils.updateLastOffsets(KafkaAppEnv.zkClient, KafkaAppEnv.groupId, offsetRanges)
    }

    ssc.checkpoint(KafkaAppEnv.checkPointDir)
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