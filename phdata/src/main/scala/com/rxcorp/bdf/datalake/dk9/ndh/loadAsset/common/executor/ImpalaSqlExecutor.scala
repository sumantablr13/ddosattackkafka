package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import java.sql.{Connection, SQLException}

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.impala.MDataTblLoadLog
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{ImpalaPartitionTable, ImpalaTable}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.ImpalaSqlQuery
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppLogger, TimestampConverter}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * Created by Shiddesha.Mydur on 3rd Jul 2019
  *
  */
class ImpalaSqlExecutor(logger: AppLogger, spark: SparkSession, jdbcConnection: Connection) extends SqlExecutor{
  override val executorName: String = "Impala SQL Executor"

  override type T_SqlQueryType = ImpalaSqlQuery
  override type T_TableType = ImpalaTable
  override type T_PartitionTableType = ImpalaPartitionTable

  final override protected def getDataFrameOnly(sqlStatement: String): DataFrame = {
    val rs = try {
      jdbcConnection.createStatement.executeQuery(sqlStatement)
    }
    catch {
      case e: SQLException => {
        logger.logError(e)
        jdbcConnection.close()
        throw e
      }
    }
    val schema = JdbcUtils.getSchema(rs, JdbcDialects.get(jdbcConnection.getMetaData.getURL))
    val rowList = JdbcUtils.resultSetToRows(rs, schema).toList
    return spark.createDataFrame(rowList.asJava, schema)
  }

  final override protected def executeSqlOnly(sqlStatement: String): Boolean = {
    try {
      jdbcConnection.createStatement.execute(sqlStatement)
    }
    catch {
      case e: SQLException => {
        logger.logError(e)
        jdbcConnection.close()
        throw e
      }
    }
    return true
  }

  final override protected def prepareQueryNoResults(sqlQuery: ImpalaSqlQuery): ImpalaSqlQuery = {
    return new ImpalaSqlQuery {
      val sqlStatement = s"${sqlQuery.sqlStatement} LIMIT 0"
      val logMessage = s"Trying SQL with 0 rows returned --> ${sqlQuery.logMessage}"
    }
  }

  final override protected def logSql(sqlQuery: ImpalaSqlQuery): Boolean = {
    logger.logMessage(sqlQuery.logMessage, sqlQuery.sqlStatement)
  }

  // This is the Impala implementation of createTable execution
  override def createTable(table: ImpalaTable): Boolean = {
    executeSql(table.sqlCreateTable)
  }

  // This is the Impala implementation of dropTable executionTable.scala
  override def dropTable(table: ImpalaTable): Boolean = {
    executeSql(table.sqlDropTable)
  }

  // This is the Impala implementation of truncateTable execution
  override def truncateTable(table: ImpalaTable): Boolean = {
    executeSql(table.sqlTruncateTable)
  }

  // This is the Impala implementation of insertTable execution
  override def insertTable(
                            table: ImpalaTable,
                            sqlQuery: ImpalaSqlQuery,
                            assetCode: String,
                            dataContextCode: String,
                            sourcePublTs: TimestampConverter,
                            nominalBusEffTs: TimestampConverter = TimestampConverter.TIMESTAMP_MINUS_INFINITY,
                            nominalBusExpryTs: TimestampConverter = TimestampConverter.TIMESTAMP_PLUS_INFINITY
                          ): Boolean = {
    executeSql(table.sqlInsertTable(sqlQuery))
    addMetadataTableLoadLog(table, assetCode, dataContextCode, sourcePublTs, nominalBusEffTs, nominalBusExpryTs)
  }

  // This is the Impala implementation of insertOverwriteTable execution
  override def insertOverwriteTable(
                                     table: ImpalaPartitionTable,
                                     sqlQuery: ImpalaSqlQuery,
                                     assetCode: String,
                                     dataContextCode: String,
                                     sourcePublTs: TimestampConverter,
                                     nominalBusEffTs: TimestampConverter = TimestampConverter.TIMESTAMP_MINUS_INFINITY,
                                     nominalBusExpryTs: TimestampConverter = TimestampConverter.TIMESTAMP_PLUS_INFINITY
                                   ): Boolean = {
    executeSql(table.sqlInsertOverwriteTable(sqlQuery))
    addMetadataTableLoadLog(table, assetCode, dataContextCode, sourcePublTs, nominalBusEffTs, nominalBusExpryTs)
  }

  // This is the Impala implementation of dropPartition execution
  override def dropPartition(table: ImpalaPartitionTable, partitionSpec: String): Boolean = {
    executeSql(table.sqlDropPartition(partitionSpec))
  }

  // This is the Spark implementation of renamePartition execution
  override def renamePartition(table: ImpalaPartitionTable, oldPartitionSpec: String, newPartitionSpec: String): Boolean =
    executeSql(table.sqlRenamePartition(oldPartitionSpec, newPartitionSpec))

  // This is the Impala implementation of calculateStatistics execution
  override def calculateStatistics(table: ImpalaTable) = {
    executeSql(table.sqlRefreshMetadata)
    executeSql(table.sqlCalculateStatistics)
  }

  // This is the Impala implementation of calculateStatistics execution for partition table
  override def calculateStatistics(table: ImpalaPartitionTable, partitionSpec: String) : Boolean = {
    executeSql(table.sqlRefreshMetadataPartition(partitionSpec))
    executeSql(table.sqlCalculateStatistics(partitionSpec))
  }

  def invalidateMetadata(table: ImpalaTable) = {
    executeSql(table.sqlInvalidateMetadata)
  }

  def refreshMetadata(table: ImpalaTable) = {
    executeSql(table.sqlRefreshMetadata)
  }

  def getMetadataTableLoadLog(databaseName: String): MDataTblLoadLog = {
    new MDataTblLoadLog(databaseName)
  }

  override protected def addMetadataTableLoadLog(
                                                  table: ImpalaTable,
                                                  assetCode: String,
                                                  dataContextCode: String,
                                                  sourcePublTs: TimestampConverter,
                                                  nominalBusEffTs: TimestampConverter,
                                                  nominalBusExpryTs: TimestampConverter
                                                ): Boolean = {
    val mDataTblLoadLog = getMetadataTableLoadLog(table.databaseName)
    val sqlQuery = new ImpalaSqlQuery {
      override val sqlStatement: String = mDataTblLoadLog.helperInsertStatement(table, dataContextCode, spark.sparkContext.applicationId, nominalBusEffTs, nominalBusExpryTs, sourcePublTs, assetCode)
      override val logMessage: String = s"Logging load operation in metadata for table ${table.logicalName}(${table.tableName})"
    }
    executeSql(mDataTblLoadLog.sqlInsertTable(sqlQuery))
  }
}