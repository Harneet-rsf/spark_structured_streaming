package com.datalamp.spark.sample

import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import kafka.consumer._
import kafka.api.{PartitionOffsetRequestInfo, OffsetRequest}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition

trait OffsetsStore extends Serializable {

  def readOffsets(topic: String): Option[Map[TopicPartition, Long]]

  def saveOffsets(topic: String, rdd: RDD[_]): Unit
  
  /**
   * Option                                 Description                           <br>
    ------                                 -----------                            <br>
    --broker-list <String: hostname:       REQUIRED: The list of hostname and     <br>
      port,...,hostname:port>                port of the server to connect to.    <br>
    --max-wait-ms <Integer: ms>            The max amount of time each fetch      <br>
                                             request waits. (default: 1000)       <br>
    --offsets <Integer: count>             number of offsets returned (default: 1)<br>
    --partitions <String: partition ids>   comma separated list of partition ids. <br>
                                             If not specified, it will find       <br>
                                             offsets for all partitions (default: <br>
                                             )                                    <br>
    --time <Long: timestamp/-1(latest)/-2  timestamp of the offsets before that   <br>
      (earliest)>                            (default: -1)                        <br>
    --topic <String: topic>                REQUIRED: The topic to get offset from.<br>
   * 
   */
  def readEndOffsetsFromKafka(brokerList: String, topic: String, partitionCSV: String, time: Int = -2, 
      nOffsets: Int = 1, clientId: String = "GetOffsetShell", maxWaitMs: Int = 5000): Option[Map[TopicPartition, Long]] = {
    
    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)

    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs).topicsMetadata
    if(topicsMetadata.size != 1 || !topicsMetadata(0).topic.equals(topic)) {
      System.err.println(("Error: no valid topic metadata for topic: %s, " + " probably the topic does not exist, run ").format(topic) +
        "kafka-list-topic.sh to verify")
      System.exit(1)
    }
        
	  val partitions =
      if(partitionCSV == "") {
        topicsMetadata.head.partitionsMetadata.map(_.partitionId)
      } else {
        partitionCSV.split(",").map(_.toInt).toSeq
      }
	  
	  var map: Map[TopicPartition, Long] = Map()
	  
    partitions.foreach { partitionId =>
      val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partitionId)
      partitionMetadataOpt match {
        case Some(metadata) =>
          metadata.leader match {
            case Some(leader) =>
              val consumer = new SimpleConsumer(leader.host, leader.port, 10000, 100000, clientId)
              val topicAndPartition = new TopicAndPartition(topic, partitionId)
              val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, nOffsets)))
              val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets

              println("%s:%d:%s".format(topic, partitionId, offsets.mkString(",")))
              map = map.+(new TopicPartition(topic, partitionId) -> offsets(0))
              
            case None => System.err.println("Error: partition %d does not have a leader. Skip getting offsets".format(partitionId))
          }
        case None => System.err.println("Error: partition %d does not exist".format(partitionId))
      }
    }
	  
//    Some(map)
	  Option(map)
  }
}
