package com.datalamp.spark.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/**
 * @author ${user.name}
 */
object StructuredStream {
  
  def main(args : Array[String]) {
    
    val spark = SparkSession.builder()
      .appName("Structured Streaming")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    
    import org.apache.spark.sql.ForeachWriter
    val writer = new ForeachWriter[(String, String)] {
      override def open(partitionId: Long, version: Long) = true
      override def process(data: (String, String)) = {
        
        Thread.sleep((Math.random() * 3000).toLong)
        
        println(data._2)
        }
      override def close(errorOrNull: Throwable) = {}
    }
    
    val df = spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "test6").
      option("startingOffsets", "earliest").
      option("maxOffsetsPerTrigger", "100").
      load()
    
    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].
      writeStream.
      foreach(writer).
      option("checkpointLocation", "/home/harneet/workspace_scala/StructuredStreaming/checkpoint").
      trigger(Trigger.Continuous(1000)).
      start()
      
    /*val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].
      writeStream.
      format("console").        // can be "orc", "json", "csv", etc.
      option("checkpointLocation", "/home/harneet/workspace_scala/StructuredStreaming/checkpoint").
      trigger(Trigger.Continuous(1000)).
      start()*/

/*    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].
      writeStream.
      format("json").        // can be "orc", "json", "csv", etc.
      option("checkpointLocation", "/home/harneet/workspace_scala/StructuredStreaming/checkpoint").
      option("path", "/home/harneet/workspace_scala/StructuredStreaming/data").
      start()*/
    
    query.awaitTermination()
  }

}
