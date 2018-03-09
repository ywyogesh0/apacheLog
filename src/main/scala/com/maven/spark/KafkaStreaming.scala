package com.maven.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._
import LogEnums._

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.types.Decimal

import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs

// Schema
// (uuid: UUID, ip_host: String, date: String, time: String, method: String, url: String, version: String, code: Int)

/** Listening for apacheLog data from Kafka's apacheLog topic on port 6667. */
object KafkaStreaming {

  case class ApacheLog(uuid: String, ip_host: String, date: String, time: String, method: String, url: String, protocol: String, version: String, code: Int, six_hour_slot: Int)

  def main(args: Array[String]) {

    // Set up the CASSANDRA host address
    val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").setAppName("LOG_ANALYTICS1")

    // Creating streaming context with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(1))

    setupLogging()

    // Constructing a regular expression to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka's brokers
    val kafkaParams = Map("metadata.broker.list" -> "sandbox.hortonworks.com:6667")

    // List of topics you want to listen for from Kafka
    val topics = List("weblogtopic1").toSet

    // Create our Kafka's stream, which will contain (topic, message) pairs.
    // map(_._2) at the end in order to only get the messages.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

    // Create log parameters Array from each log line
    val logParamArray = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches())
        Array(matcher.group(1), matcher.group(4), matcher.group(5), matcher.group(6))
      else
        Array("ERROR")
    })

    // Convert DStream[String] -> DStream[(,,,,,,)]
    val apacheLogStream = logParamArray.map(arr => {
      
      // val arr: Array[String] = line.split(" ")
      
      if (arr.size == 4) {
        
        val time = extractLogParams(arr(1), TIME)
        val six_hour_slot = extractHourSlot(time)
        
        ApacheLog(
          UUIDs.timeBased().toString(),
          arr(0), converDateStringFormat(extractLogParams(arr(1), DATE)), time,
          extractLogParams(arr(2), METHOD), extractLogParams(arr(2), URL),
          extractLogParams(arr(2), PROTOCOL), extractLogParams(arr(2), VERSION), arr(3).toInt, six_hour_slot)
      } else
        ApacheLog(UUIDs.timeBased().toString(), "error", "error", "error", "error", "error", "error", "0.0", 0, 0)
    })

    // Now store it in CASSANDRA
    apacheLogStream.foreachRDD((rdd, time) => {
      rdd.cache()
      println("Writing " + rdd.count() + " rows to Cassandra...")
      rdd.saveToCassandra("loganalytics", "apachelogs", SomeColumns("uuid", "ip_host", "date", "time", "method", "url", "protocol", "version", "code", "six_hour_slot"))
    })

    // Kick it off
    ssc.checkpoint("file:///home/maria_dev/streaming_checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}

