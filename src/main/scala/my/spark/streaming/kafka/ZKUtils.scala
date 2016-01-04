package my.spark.streaming.kafka

import java.util.Properties

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.spark.annotation.Experimental
import org.apache.spark.streaming.kafka.OffsetRange

import kafka.common.TopicAndPartition
import kafka.utils.VerifiableProperties
import kafka.utils.ZKConfig
import kafka.utils.ZKGroupDirs

/**
 * @author hammertank
 *
 * modified from kafka.utils.ZkUtils
 *
 */
object ZKUtils extends Serializable {

  def createZKClient(zkQuorum: String): ZkClient = {
    val props = new Properties
    props.put("zookeeper.connect", zkQuorum)
    props.put("zookeeper.connection.timeout.ms", "10000")

    val zkConfig = new ZKConfig(new VerifiableProperties(props))

    new ZkClient(zkConfig.zkConnect, zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs)
  }

  private def toPath(groupId: String) = {
    val groupDirs = new ZKGroupDirs(groupId)
    s"${groupDirs.consumerGroupDir}/lastoffsets"
  }

  def fetchLastOffsets(client: ZkClient, groupId: String): Option[Map[TopicAndPartition, Long]] = {
    try {
      Some(client.readData(toPath(groupId)))
    } catch {
      case e: ZkNoNodeException =>
        (None)
      case e2: Throwable => throw e2
    }
  }

  def updateLastOffsets(client: ZkClient, groupId: String, offsetRange: Array[OffsetRange]) {
    val data = offsetRange.map { x => TopicAndPartition(x.topic, x.partition) -> x.untilOffset }.toMap

    val path = toPath(groupId)

    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        } catch {
          case e: ZkNodeExistsException =>
            client.writeData(path, data)
          case e2: Throwable => throw e2
        }
      }
      case e2: Throwable => throw e2
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }
}