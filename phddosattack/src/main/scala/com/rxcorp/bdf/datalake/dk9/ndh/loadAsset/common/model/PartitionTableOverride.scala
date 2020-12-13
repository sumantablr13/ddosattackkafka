package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model

trait PartitionTableOverride extends PartitionTable {
  override lazy val sqlCreateTableStatement: String =
    s"CREATE TABLE IF NOT EXISTS ${tableName} (${tableSchema}) PARTITIONED BY (${partitionSchema}) ${createTableOpts}"

  override lazy val sqlCalculateStatisticsStatement = s"ANALYZE TABLE ${tableName} PARTITION (${partitionColumns.map(col => col._1).mkString(", ")}) COMPUTE STATISTICS"

  override def sqlInsertTableStatement(sqlStatement: String) = s"INSERT INTO ${tableName} PARTITION (${partitionColumns.map(col => col._1).mkString(", ")}) ${sqlStatement}"
}
