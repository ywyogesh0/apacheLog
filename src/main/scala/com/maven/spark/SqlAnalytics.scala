package com.maven.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._

import com.datastax.spark.connector._
import com.datastax._

object SqlAnalytics {

  case class ApacheLog(uuid: String, ip_host: String, date: String, time: String, method: String, url: String, protocol: String, version: String, code: Int, six_hour_slot: Int)
  case class Temp1(date: String, url: String, count: BigInt)
  case class Temp2(date: String, url: String, six_hour_slot: Int, count: BigInt)
  case class Temp3(ip_host: String, date: String, url: String, count: BigInt)
  case class Temp4(ip_host: String, date: String, url: String, six_hour_slot: Int, count: BigInt)

  val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").setAppName("SqlAnalyticsJob")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._

  val cassandraRDD = spark.sparkContext.cassandraTable[ApacheLog]("loganalytics", "apachelogs")
  val ds1 = cassandraRDD.toDS()

  ds1.cache()
  val ds2 = ds1;

  def main(args: Array[String]) {

    ds1.createOrReplaceTempView("log")

    // Connect C-a-s-s-a-n-d-r-a - 2nd Way
    /*val ds2 = sparkSession.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "apachelogs", "keyspace" -> "loganalytics")).load.as[ApacheLog].cache()*/

    log_analytics_1(spark)
    log_analytics_2(spark)
    log_analytics_3(spark)

    spark.stop()
  }

  // 1st Problem SQL Query - (excluding images - only JPG and JPEG) - case insensitive
  def log_analytics_1(spark: SparkSession) {

    ds2.filter(log => {
      
      val iUrl = log.url.toLowerCase()
      !(iUrl.endsWith(".jpg") || iUrl.endsWith(".jpeg"))
    
    }).createOrReplaceTempView("log_2")

    // Windowing for 24 hours
    val logAnalytics1_1 = spark.sql("SELECT date, url, count(1) AS count FROM log_2 GROUP BY date, url ORDER BY date, count DESC").as[Temp1]

    logAnalytics1_1.createOrReplaceTempView("logAnalytics1_1_View")

    val logAnalytics1_1_Result = spark.sql("SELECT * from (SELECT date, url, count, DENSE_RANK() OVER (PARTITION BY date ORDER BY count DESC) AS rank FROM logAnalytics1_1_View) tmp WHERE rank <= 3 ORDER BY date, count DESC").coalesce(1).cache()

    // logAnalytics1_1_Result.show()
    logAnalytics1_1_Result.write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/maria_dev/logAnalytics1_24")

    // ------------------------------------------------------------------------------------------

    // Windowing for 6 hours
    val logAnalytics1_2 = spark.sql("SELECT date, url, six_hour_slot, count(1) AS count FROM log GROUP BY date, url, six_hour_slot ORDER BY date, six_hour_slot, count DESC").as[Temp2]

    logAnalytics1_2.createOrReplaceTempView("logAnalytics1_2_view")

    val logAnalytics1_2_Result = spark.sql("SELECT * from (SELECT date, url, six_hour_slot, count, DENSE_RANK() OVER (PARTITION BY date, six_hour_slot ORDER BY count DESC) AS rank FROM logAnalytics1_2_view) tmp WHERE rank <= 3 ORDER BY date, six_hour_slot, count DESC").coalesce(1).cache()

    // logAnalytics1_2_Result.show()
    logAnalytics1_2_Result.write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/maria_dev/logAnalytics1_6")
  }

  // 2nd Problem SQL Query
  def log_analytics_2(spark: SparkSession) {

    // Windowing for 24 hours
    val logAnalytics2_1 = spark.sql("SELECT ip_host, date, url, count(1) AS count FROM log GROUP BY ip_host, date, url ORDER BY date, count DESC").as[Temp3]

    logAnalytics2_1.createOrReplaceTempView("logAnalytics2_1_View")

    val logAnalytics2_1_Result = spark.sql("SELECT * from (SELECT ip_host, date, url, count, DENSE_RANK() OVER (PARTITION BY date ORDER BY count DESC) AS rank FROM logAnalytics2_1_View) tmp WHERE rank <= 3 ORDER BY date, count DESC").coalesce(1).cache()

    // logAnalytics2_1_Result.show()
    logAnalytics2_1_Result.write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/maria_dev/logAnalytics2_24")

    // ------------------------------------------------------------------------------------------

    // Windowing for 6 hours
    val logAnalytics2_2 = spark.sql("SELECT ip_host, date, url, six_hour_slot, count(1) AS count FROM log GROUP BY ip_host, date, url, six_hour_slot ORDER BY date, six_hour_slot, count DESC").as[Temp4]

    logAnalytics2_2.createOrReplaceTempView("logAnalytics2_2_view")

    val logAnalytics2_2_Result = spark.sql("SELECT * from (SELECT ip_host, date, url, six_hour_slot, count, DENSE_RANK() OVER (PARTITION BY date, six_hour_slot ORDER BY count DESC) AS rank FROM logAnalytics2_2_view) tmp WHERE rank <= 2 ORDER BY date, six_hour_slot, count DESC").coalesce(1).cache()

    // logAnalytics2_2_Result.show()
    logAnalytics2_2_Result.write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/maria_dev/logAnalytics2_6")
  }

  // 3rd Problem SQL Query
  def log_analytics_3(spark: SparkSession) {

    // Test
    val logAnalytics3 = spark.sql("SELECT version, code, count(*) AS count FROM log WHERE version IN ('1.0','1.1') GROUP BY code, version ORDER BY count DESC").coalesce(1).cache()

    // Production - remove coalesce(n)

    // logAnalytics3.show()
    logAnalytics3.write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/maria_dev/log_analytics_3")
  }

}