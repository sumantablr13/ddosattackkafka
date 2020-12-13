package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import java.sql.Connection

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppLogger
import org.apache.spark.sql.SparkSession

class ImpalaExecutor(logger: AppLogger, spark: SparkSession, jdbcConnection: Connection){
  lazy val sql = new ImpalaSqlExecutor(logger, spark, jdbcConnection)
}
