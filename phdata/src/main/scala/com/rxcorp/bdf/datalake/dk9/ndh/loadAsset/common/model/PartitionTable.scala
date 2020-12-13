package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model

abstract class PartitionTable(databaseName: String) extends Table(databaseName: String) {

  val partitionColumns: Array[(String,String)]
  final val partitionSchema: String = partitionColumns.map(partitionColumn => s"${partitionColumn._1} ${partitionColumn._2}").mkString(", ")

  def sqlInsertOverwriteTableStatement(sqlStatement: String) = s"INSERT OVERWRITE TABLE ${tableName} PARTITION (${partitionColumns.map(col => col._1).mkString(", ")}) ${sqlStatement}"
  def sqlInsertOverwriteTableLogMessage(logMessage: String) = s"Inserting into table: ${tableName} with partition overwrite --> ${logMessage}"
  def sqlInsertOverwriteTable(sqlQuery: T_SqlType): T_SqlType

  def sqlDropPartitionStatement(partitionSpec: String) = s"ALTER TABLE ${tableName} DROP IF EXISTS PARTITION (${partitionSpec})"
  def sqlDropPartitionLogMessage(partitionSpec: String) = s"Dropping partition: (${partitionSpec}) from table: ${tableName}"
  def sqlDropPartition(partitionSpec: String): T_SqlType

  def sqlCalculateStatisticsPartitionLogMessage(partitionSpec: String) = s"Calculating partition statistics: (${partitionSpec}) from table: ${tableName}"
  def sqlCalculateStatisticsPartitionStatement(partitionSpec: String) = s"ANALYZE TABLE ${tableName} PARTITION (${partitionSpec}) COMPUTE STATISTICS"
  def sqlCalculateStatistics(partitionSpec: String): T_SqlType

  def sqlRenamePartitionStatement( oldPartitionSpec: String, newPartitionSpec: String ) = s"ALTER TABLE ${tableName} PARTITION ( ${oldPartitionSpec} ) RENAME TO PARTITION ( ${newPartitionSpec} )"
  def sqlRenamePartitionLogMessage( oldPartitionSpec: String, newPartitionSpec: String ) = s"Renaming Partition from ( ${oldPartitionSpec} ) to (${newPartitionSpec}) for table: ${tableName}"
  def sqlRenamePartition( oldPartitionSpec: String, newPartitionSpec: String  ): T_SqlType
}