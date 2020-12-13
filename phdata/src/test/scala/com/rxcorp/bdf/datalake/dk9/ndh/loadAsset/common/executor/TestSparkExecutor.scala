package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppLogger
import org.apache.spark.sql.SparkSession

class TestSparkExecutor( spark: SparkSession, kafkaServer: String) extends SparkExecutor( spark, kafkaServer) {
  def getSparkSession(): SparkSession =
  {
    return spark
  }
}
