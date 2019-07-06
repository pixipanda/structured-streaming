package com.pixipanda.structuredstreaming.kakfa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}


object VechicleTypeCountWindowWaterMark  {

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


    val iotstreamdf  = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "iottraffic")
      .option("startingOffsets", "earliest")
      .load()


    val trafficstream = iotstreamdf.withColumn("traffic", // nested structure with our json
      from_json($"value".cast(StringType), schema))
      .selectExpr("traffic.*", "partition", "offset")
      .withColumn("eventTime", unix_timestamp('time, "EEE MMM dd HH:mm:ss zzz yyyy").cast(TimestampType))


    val vechicleTypeWindowCount = trafficstream
      .dropDuplicates("vehicleId")
      .withWatermark("eventTime", "5 seconds")
      .groupBy(window('eventTime, "30 seconds"), 'vehicleType)
      .count()
      .orderBy("window")

    //Watermark will not delete the state in Complete Output Mode
    val query = vechicleTypeWindowCount.writeStream.format("console").option("truncate", "false").outputMode(OutputMode.Complete())

    query.start().awaitTermination()
  }
}