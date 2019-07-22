package com.pixipanda.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}


object  VechicleTypeWindowCount {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._


    val schema = new StructType().add("vehicleId", "string")
      .add("vehicleType", "string")
      .add("routeId", "string")
      .add("latitude", "string")
      .add("longitude", "string")
      .add("time", "string")
      .add("speed", "string")
      .add("fuelLevel", "string")


    val socketStreamDf = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)
      .load()


    val trafficstream = socketStreamDf.withColumn("traffic", // nested structure with our json
      from_json($"value".cast(StringType), schema))
      .selectExpr("traffic.*")
      .withColumn("eventTime", unix_timestamp('time, "EEE MMM dd HH:mm:ss zzz yyyy").cast(TimestampType))

    val vechicleTypeRouteWindowCount = trafficstream
      .dropDuplicates("vehicleId")
      .groupBy(window('eventTime, "1 minute"),  'vehicleType)
      .count()
      .orderBy("window")

    val query = vechicleTypeRouteWindowCount.writeStream.format("console").option("truncate", "false").outputMode(OutputMode.Complete())

    query.start().awaitTermination()
  }
}