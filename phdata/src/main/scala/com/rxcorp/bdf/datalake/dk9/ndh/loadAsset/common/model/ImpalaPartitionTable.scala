package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.ImpalaSqlQuery

trait ImpalaPartitionTable extends PartitionTable with ImpalaTable with PartitionTableOverride {

  override def sqlCalculateStatisticsPartitionStatement(partitionSpec: String) = s"COMPUTE INCREMENTAL STATS ${tableName} PARTITION (${partitionSpec})"

  override def sqlInsertOverwriteTable(sqlQuery: ImpalaSqlQuery) = new ImpalaSqlQuery {
    val sqlStatement = sqlInsertOverwriteTableStatement(sqlQuery.sqlStatement)
    val logMessage = sqlInsertOverwriteTableLogMessage(sqlQuery.logMessage)
  }

  override def sqlDropPartition(partitionSpec: String) = new ImpalaSqlQuery {
    val sqlStatement = sqlDropPartitionStatement(partitionSpec)
    val logMessage = sqlDropPartitionLogMessage(partitionSpec)
  }

  override def sqlRenamePartition(oldPartitionSpec: String, newPartitionSpec: String) = new ImpalaSqlQuery {
    val sqlStatement = sqlRenamePartitionStatement(oldPartitionSpec, newPartitionSpec)
    val logMessage = sqlRenamePartitionLogMessage(oldPartitionSpec, newPartitionSpec)
  }

  override def sqlCalculateStatistics(partitionSpec: String) = new ImpalaSqlQuery {
    val sqlStatement = sqlCalculateStatisticsPartitionStatement(partitionSpec)
    val logMessage = sqlCalculateStatisticsPartitionLogMessage(partitionSpec)
  }

  def sqlRefreshMetadataPartitionStatement(partitionSpec: String) = s"REFRESH ${tableName} PARTITION (${partitionSpec})"
  def sqlRefreshMetadataPartitionLogMessage(partitionSpec: String) = s"Refreshing Impala metadata on table ${tableName} for partition (${partitionSpec})"

  def sqlRefreshMetadataPartition(partitionSpec: String) = new ImpalaSqlQuery {
    val sqlStatement = sqlRefreshMetadataPartitionStatement(partitionSpec: String)
    val logMessage = sqlRefreshMetadataPartitionLogMessage(partitionSpec: String)
  }
}
