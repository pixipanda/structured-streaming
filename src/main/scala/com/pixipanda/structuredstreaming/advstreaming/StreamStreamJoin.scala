package com.pixipanda.structuredstreaming.advstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, unix_timestamp}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.functions.expr
object StreamStreamJoin {


  def main(args: Array[String]) {


    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._


    val adImpressionschema = new StructType().add("id", "string")
      .add("timestamp", "string")
      .add("publisher", "string")
      .add("advertiser", "string")
      .add("website", "string")
      .add("geo", "string")
      .add("bid", "string")
      .add("cookie", "string")


    val adClickSchema = new StructType().add("id", "string")
      .add("timestamp", "string")


    val adImpressionRawStreamDF = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "adImpression")
      .option("startingOffsets", "earliest")
      .load()


    val adClickRawStreamDF = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "adClick")
      .option("startingOffsets", "earliest")
      .load()


    val adImpressionStream = adImpressionRawStreamDF.withColumn("adImpression", // nested structure with our json
      from_json($"value".cast(StringType), adImpressionschema))
      .selectExpr("adImpression.*", "partition", "offset")
      .withColumnRenamed("id", "impressionAdId")
      .withColumn("impressionTime", unix_timestamp('timestamp, "EEE MMM dd HH:mm:ss zzz yyyy").cast(TimestampType))
      .drop("timestamp")

    val adClickStream = adClickRawStreamDF.withColumn("adClick", // nested structure with our json
      from_json($"value".cast(StringType), adClickSchema))
      .selectExpr("adClick.*", "partition", "offset")
      .withColumnRenamed("id", "clickAdId")
      .withColumn("clickTime", unix_timestamp('timestamp, "EEE MMM dd HH:mm:ss zzz yyyy").cast(TimestampType))
      .drop("timestamp")



    val joinedStream = adImpressionStream.join(
      adClickStream,
      expr("""
      clickAdId = impressionAdId AND
      clickTime >= impressionTime
      """
      )
    )

    StreamingDataFrameWriter.StreamingDataFrameConsoleWriter(joinedStream, "Ad_Impression_Click_Console")

    sparkSession.streams.awaitAnyTermination()
  }
}
