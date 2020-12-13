package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.TimestampConverter
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SqlQuery
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{PartitionTable, Table}
import org.apache.spark.sql.DataFrame

abstract class SqlExecutor extends Executor {

  type T_SqlQueryType <: SqlQuery
  type T_TableType <: Table
  type T_PartitionTableType <: PartitionTable

  protected def executeSqlOnly(sqlStatement: String): Boolean
  protected def prepareQueryNoResults(sqlQuery: T_SqlQueryType): T_SqlQueryType
  protected def getDataFrameOnly(sqlStatement: String): DataFrame
  protected def logSql(sqlQuery: T_SqlQueryType): Boolean

  protected def addMetadataTableLoadLog(
                                         table: T_TableType,
                                         assetCode: String,
                                         dataContextCode: String,
                                         sourcePublTs: TimestampConverter,
                                         nominalBusEffTs: TimestampConverter,
                                         nominalBusExpryTs: TimestampConverter
                                       ): Boolean

  final def executeSql(sqlQuery: T_SqlQueryType): Boolean = {
    logSql(sqlQuery)
    val ret = executeSqlOnly(sqlQuery.sqlStatement)
    return ret
  }

  def createTable(table: T_TableType): Boolean
  def dropTable(table: T_TableType): Boolean
  def truncateTable(table: T_TableType): Boolean
  def insertTable(
                   table: T_TableType,
                   sqlQuery: T_SqlQueryType,
                   assetCode: String,
                   dataContextCode: String,
                   sourcePublTs: TimestampConverter,
                   nominalBusEffTs: TimestampConverter,
                   nominalBusExpryTs: TimestampConverter
                 ): Boolean
  def insertOverwriteTable(
                            table: T_PartitionTableType,
                            sqlQuery: T_SqlQueryType,
                            assetCode: String,
                            dataContextCode: String,
                            sourcePublTs: TimestampConverter,
                            nominalBusEffTs: TimestampConverter,
                            nominalBusExpryTs: TimestampConverter
                          ): Boolean
  def dropPartition(table: T_PartitionTableType, partitionSpec: String): Boolean
  def renamePartition(table: T_PartitionTableType, oldPartitionSpec: String, newPartitionSpec: String): Boolean
  def calculateStatistics(table: T_TableType): Boolean
  def calculateStatistics(table: T_PartitionTableType, partitionSpec: String): Boolean

  final def getDataFrame(sqlQuery: T_SqlQueryType): DataFrame = {
    logSql(sqlQuery)
    val df = getDataFrameOnly(sqlQuery.sqlStatement)
    df
  }
}
