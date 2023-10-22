package com.datalamp.spark.sample

import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import scala.io.Source
import java.io.FileNotFoundException
import java.util.Date
import org.apache.kafka.common.TopicPartition

class FileOffsetsStore(filePath: String) extends OffsetsStore {

  // Read the previously saved offsets from File
  override def readOffsets(topic: String): Option[Map[TopicPartition, Long]] = {

    println("Reading offsets from file: " + filePath)

    var startingOffsets : String = ""
    try
    {
      for (line <- Source.fromFile(filePath).getLines)
        startingOffsets = startingOffsets.concat(line)
    }
    catch
    {
      case ex: FileNotFoundException => println(new Date() + "::: There is no offset file present, so starting from 'earliest' offset.")
      case ex: Throwable => ex.printStackTrace()
    }

    startingOffsets match {
      case "" =>
        println("No offsets found")
        None
        
      case _ =>
            println(s"Read offset ranges: ${startingOffsets}")

        val offsets = startingOffsets.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap

        println("Done reading offsets from file.")

        Some(offsets)
        }
  }

  // Save the offsets back to File
  //
  // IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
  // because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
  // logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
  // workaround.
  override def saveOffsets(topic: String, rdd: RDD[_]): Unit = {

    println("Saving offsets to File: " + filePath)

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => println(s"Using ${offsetRange}"))

    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}")
      .mkString(",")
    println(s"Writing offsets to File: ${offsetsRangesStr}")
    
    import java.io.PrintWriter
    new PrintWriter(filePath) { write(offsetsRangesStr); close }

    println("Done updating offsets")

  }

}
