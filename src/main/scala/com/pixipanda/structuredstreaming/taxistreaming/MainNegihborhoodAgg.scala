package com.pixipanda.structuredstreaming.taxistreaming

import com.pixipanda.structuredstreaming.taxistreaming.utils.{ParseKafkaMessage, StreamingDataFrameWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._


object MainNegihborhoodAgg {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("NewYork Taxi Neighbourhood Avg Tips")
      .getOrCreate()

    import spark.implicits._


    val taxiRidesSchema = StructType(Array(
      StructField("rideId", LongType), StructField("isStart", StringType),
      StructField("endTime", TimestampType), StructField("startTime", TimestampType),
      StructField("startLon", FloatType), StructField("startLat", FloatType),
      StructField("endLon", FloatType), StructField("endLat", FloatType),
      StructField("passengerCnt", ShortType), StructField("taxiId", LongType),
      StructField("driverId", LongType)))

    val taxiFaresSchema = StructType(Seq(
      StructField("rideId", LongType), StructField("taxiId", LongType),
      StructField("driverId", LongType), StructField("startTime", TimestampType),
      StructField("paymentType", StringType), StructField("tip", FloatType),
      StructField("tolls", FloatType), StructField("totalFare", FloatType)))

    var sdfRides = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "taxirides").
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(value AS STRING)")

    var sdfFares = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "taxifares").
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(value AS STRING)")

    sdfRides = ParseKafkaMessage.parseDataFromKafkaMessage(sdfRides, taxiRidesSchema)
    sdfFares = ParseKafkaMessage.parseDataFromKafkaMessage(sdfFares, taxiFaresSchema)

    sdfRides = TaxiProcessing.cleanRidesOutsideNYC(sdfRides)
    sdfRides = TaxiProcessing.removeUnfinishedRides(sdfRides)
    var sdf = TaxiProcessing.joinRidesWithFares(sdfRides, sdfFares)
    sdf = TaxiProcessing.appendStartEndNeighbourhoods(sdf, spark)

   /*val tips = sdf
   .groupBy(
     window('endTime, "30 minutes", "10 minutes"), 'stopNbhd)
   .agg(avg("tip"))*/

    StreamingDataFrameWriter.StreamingDataFrameConsoleWriter(sdf, "TipsInConsole")

    /*StreamingDataFrameWriter.StreamingDataFrameConsoleWriter(sdfFares, "TaxiFaresConsole")
    StreamingDataFrameWriter.StreamingDataFrameConsoleWriter(sdfRides, "TaxiRidesConsole")*/

    spark.streams.awaitAnyTermination()

  }
}
