package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor


import org.apache.spark.sql.SparkSession

class SparkExecutor(spark: SparkSession, kafkaServer: String)
{
  lazy val sql = new SparkSqlExecutor( spark)
  lazy val file = new FileExecutor( spark)
  lazy val kafka = new KafkaExecutor( spark, kafkaServer)
}
