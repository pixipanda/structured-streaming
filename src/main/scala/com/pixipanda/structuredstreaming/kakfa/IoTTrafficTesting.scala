package  com.pixipanda.structuredstreaming.kakfa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{TimestampType, StringType, StructType}
import org.apache.spark.sql.functions._


object IoTTrafficTesting {

  def main(args: Array[String]) {


    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    import  sparkSession.implicits._


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
      .withColumn("abc", unix_timestamp('time, "EEE MMM dd HH:mm:ss zzz yyyy").cast(TimestampType))
    //val trafficstream = iotstreamdf.select(from_json(col("value").cast("string"), schema))

    val query = iotstreamdf.writeStream.format("console").option("truncate", "false").outputMode(OutputMode.Append())

    query.start().awaitTermination()
  }
}