package com.datalamp.spark.sample

import java.io.File

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.Config

import java.io.FileNotFoundException

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.util.Arrays
import org.apache.spark.sql.DataFrame
import java.time.Instant

object KafkaConsumer {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("torcai_spark_consumer")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "id4",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "earliest")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))
    ssc.checkpoint("/home/harneet/workspace_scala/StructuredStreaming/checkpoint2")
    
    process(spark, ssc, kafkaParams)
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  def process(spark: SparkSession, ssc: StreamingContext, kafkaParams: Map[String, String]): Unit = {

    val topic = "test4"
    val offsetsStore: OffsetsStore = new FileOffsetsStore("/home/harneet/workspace_scala/StructuredStreaming/test4")
    val stream = Util.kafkaStream[String, String](ssc, kafkaParams, offsetsStore, topic)

    import spark.implicits._
    
    stream.foreachRDD { rdd =>
      
      val startTime = Instant.now().getEpochSecond
      
      if (rdd.map(_.value()).toLocalIterator.nonEmpty) {
        val df = rdd.map(_.value()).toDF()//.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        
        df.write.format("json").save("/home/harneet/workspace_scala/StructuredStreaming/data2")
        val endTime = Instant.now().getEpochSecond
        
        println("Time taken in ms: " + (endTime - startTime))
        
        offsetsStore.saveOffsets(topic, rdd)
      }
    }
  }
}