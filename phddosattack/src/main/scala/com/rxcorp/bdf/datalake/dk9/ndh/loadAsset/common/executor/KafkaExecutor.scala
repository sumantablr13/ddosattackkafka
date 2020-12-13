package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import org.apache.spark.sql._
import org.apache.spark.util.CollectionAccumulator


class KafkaExecutor( spark: SparkSession, kafkaServer: String) extends Executor {
  override val executorName: String = "Kafka Executor"

  def sendEventToKafka(jsonEvent: String, jsonDescription: String, kafkaTopic: String, customKafkaServer: String = "" ): Unit = {
    import spark.implicits._
    //logger.logMessage(s"Sending JSON event to kafka topic $kafkaTopic: $jsonDescription")
    val events = spark
      .createDataset(List(jsonEvent))
      .alias("value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", if(customKafkaServer.isEmpty) kafkaServer else customKafkaServer)
      .option("topic", kafkaTopic)
      .save()
  }

  private def formatOffsetString(topic: String, partition: Int, offset:Long): String = {
    s"""{"$topic":{"$partition":$offset}}"""
  }

  def readSingleKafkaEvent(streamTopic: String, offset: Long, customKafkaServer: String = ""): Dataset[(String, String, Int, Long)] = {
   // logger.logMessage(s"Reading from Kafka topic: $streamTopic")
    import spark.implicits._
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", if(customKafkaServer.isEmpty) kafkaServer else customKafkaServer)
      .option("subscribe", streamTopic)
      .option("startingOffsets", formatOffsetString(streamTopic,0,offset))
      .option("endingOffsets", formatOffsetString(streamTopic, 0, offset+1))
      .option("failOnDataLoss", value = false)
      .load()
      .selectExpr("CAST(value as STRING)", "CAST(topic as STRING)", "CAST(partition as INT)", "CAST(offset as LONG)")
      .as[(String, String, Int, Long)]
  }

  def readMultiKafkaEvents(streamTopic: String, customKafkaServer: String, offset: Long, mbtMultiKafkaEventsToRead: Long = 0): Dataset[(String, String, Int, Long)] = {
    import spark.implicits._
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers",  if(customKafkaServer.isEmpty) kafkaServer else customKafkaServer)
      .option("subscribe", streamTopic)
      .option("startingOffsets", formatOffsetString(streamTopic,0, offset))
      .option("endingOffsets", formatOffsetString(streamTopic, 0, offset + mbtMultiKafkaEventsToRead))
      .option("failOnDataLoss", false)
      .load()
      .selectExpr("CAST(value as STRING)", "CAST(topic as STRING)", "CAST(partition as INT)", "CAST(offset as LONG)")
      .as[(String, String, Int, Long)]
  }


  def getCollectionAccumulator[T](accumulatorName: String): CollectionAccumulator[T] =
    spark.sparkContext.collectionAccumulator[T](accumulatorName)


}
