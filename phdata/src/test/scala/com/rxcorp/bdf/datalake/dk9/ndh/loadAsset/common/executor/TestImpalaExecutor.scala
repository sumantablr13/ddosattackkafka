package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import java.sql.Connection

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppLogger
import org.apache.spark.sql.SparkSession

class TestImpalaExecutor(logger: AppLogger, spark: SparkSession, jdbcConnection: Connection, tableSuffix: String) extends ImpalaExecutor(logger, spark, jdbcConnection) {
  override lazy val sql = new TestImpalaSqlExecutor(logger, spark, jdbcConnection, tableSuffix)
}
