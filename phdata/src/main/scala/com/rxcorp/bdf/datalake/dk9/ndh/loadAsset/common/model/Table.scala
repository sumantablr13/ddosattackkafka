package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SqlQuery
import org.apache.spark.sql.types.StructType

abstract class Table(_databaseName: String) {

  type T_SqlType <: SqlQuery
  val logicalName: String
  val physicalName: String
  final val databaseName = _databaseName
  val tableColumns: Array[(String, String)]
  val schema: StructType 
  val createTableOpts: String

  final lazy val tableName = s"${_databaseName}.$physicalName"
  final lazy val tableSchema: String = tableColumns.map(tableColumn => s"${tableColumn._1} ${tableColumn._2}").mkString(",\n")

  lazy val sqlCreateTableStatement = s"CREATE TABLE IF NOT EXISTS ${tableName} (${tableSchema}) ${createTableOpts}"
  lazy val sqlCreateTableLogMessage = s"Creating table if not exists: ${tableName}"
  def sqlCreateTable: T_SqlType
  
  lazy val sqlDropTableStatement = s"DROP TABLE IF EXISTS ${tableName} PURGE"
  lazy val sqlDropTableLogMessage = s"Droping table: ${tableName}"
  def sqlDropTable: T_SqlType

  lazy val sqlTruncateTableStatement = s"TRUNCATE TABLE  ${tableName}"
  lazy val sqlTruncateTableLogMessage = s"Truncating entire table: ${tableName}"
  def sqlTruncateTable: T_SqlType

  lazy val sqlCalculateStatisticsStatement = s"ANALYZE TABLE ${tableName} COMPUTE STATISTICS"
  lazy val sqlCalculateStatisticsLogMessage = s"Calculating statistics for table: ${tableName}"
  def sqlCalculateStatistics: T_SqlType

  def sqlInsertTableStatement(sqlStatement: String) = s"INSERT INTO ${tableName} ${sqlStatement}"
  def sqlInsertTableLogMessage(logMessage: String) = s"Inserting into table: ${tableName} --> ${logMessage}"
  def sqlInsertTable(sqlQuery: T_SqlType): T_SqlType
}
