package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SparkSqlQuery

trait SparkPartitionTable extends PartitionTable with SparkTable with PartitionTableOverride {
  override def sqlInsertOverwriteTable(sqlQuery: SparkSqlQuery) = new SparkSqlQuery {
    val sqlStatement = sqlInsertOverwriteTableStatement(sqlQuery.sqlStatement)
    val logMessage = sqlInsertOverwriteTableLogMessage(sqlQuery.logMessage)
  }

  override def sqlDropPartition(partitionSpec: String) = new SparkSqlQuery {
    val sqlStatement = sqlDropPartitionStatement(partitionSpec)
    val logMessage = sqlDropPartitionLogMessage(partitionSpec)
  }

  override def sqlCalculateStatistics(partitionSpec: String) = new SparkSqlQuery {
    val sqlStatement = sqlCalculateStatisticsPartitionStatement(partitionSpec)
    val logMessage = sqlCalculateStatisticsPartitionLogMessage(partitionSpec)
  }

  override def sqlRenamePartition(oldPartitionSpec: String, newPartitionSpec: String) = new SparkSqlQuery {
    val sqlStatement = sqlRenamePartitionStatement(oldPartitionSpec, newPartitionSpec)
    val logMessage = sqlRenamePartitionLogMessage(oldPartitionSpec, newPartitionSpec)
  }

}
