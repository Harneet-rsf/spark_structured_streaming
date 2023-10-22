package com.datalamp.spark.sample


import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.api.OffsetRequest
import kafka.consumer.SimpleConsumer
import kafka.api.PartitionOffsetRequestInfo
import scala.reflect.ClassTag
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
//import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.sql.types.StructType
import org.apache.avro.Schema
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord


object Util {
  
  def schemaFromString(json: String): Schema = {
    val parser = new Schema.Parser()
    parser parse json
  }

  def kafkaStream[K: ClassTag, V: ClassTag]
        (ssc: StreamingContext, kafkaParams: Map[String, String], offsetsStore: OffsetsStore, topic: String): InputDStream[ConsumerRecord[K, V]] = {
    
    val topics = Set(topic)
    val storedOffsets = offsetsStore.readOffsets(topic)
    
    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets
//        KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
          KafkaUtils.createDirectStream[K, V](
          ssc,
          PreferConsistent,
          consumerStrategy = ConsumerStrategies.Subscribe[K, V](topics, kafkaParams))

      case Some(fromOffsets) =>
        // start from previously saved offsets
//        val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
//        KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, fromOffsets, messageHandler)
        
          KafkaUtils.createDirectStream[K, V](
          ssc,
          PreferConsistent,
          consumerStrategy = ConsumerStrategies.Subscribe[K, V](topics, kafkaParams, fromOffsets))
    }
    
//    // save the offsets
//    kafkaStream.foreachRDD(rdd => offsetsStore.saveOffsets(topic, rdd))
    
    kafkaStream
  }

  // Kafka input stream
  def kafkaStream[K: ClassTag, V: ClassTag]
      (ssc: StreamingContext, brokers: String, offsetsStore: OffsetsStore, topic: String): InputDStream[ConsumerRecord[K, V]] =
    kafkaStream(ssc, Map("metadata.broker.list" -> brokers), offsetsStore, topic)

}
