package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import java.sql.Connection

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppLogger
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.impala.{MDataTblLoadLog, TestMDataTableLoadLog}
import org.apache.spark.sql.SparkSession

class TestImpalaSqlExecutor(logger: AppLogger, spark: SparkSession, jdbcConnection: Connection, tableSuffix: String) extends ImpalaSqlExecutor(logger, spark, jdbcConnection) {

  override def getMetadataTableLoadLog(databaseName: String): MDataTblLoadLog = {
    new TestMDataTableLoadLog(databaseName, tableSuffix)
  }
}
