package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.ImpalaSqlQuery

trait ImpalaTable extends Table {
  override type T_SqlType = ImpalaSqlQuery

  override lazy val sqlCreateTable = new ImpalaSqlQuery {
    val sqlStatement = sqlCreateTableStatement
    val logMessage = sqlCreateTableLogMessage
  }

  override lazy val sqlDropTable = new ImpalaSqlQuery {
    val sqlStatement = sqlDropTableStatement
    val logMessage = sqlDropTableLogMessage
  }

  override lazy val sqlTruncateTable = new ImpalaSqlQuery {
    val sqlStatement = sqlTruncateTableStatement
    val logMessage = sqlTruncateTableLogMessage
  }

  override lazy val sqlCalculateStatisticsStatement = s"COMPUTE STATS ${tableName}"
  
  override def sqlCalculateStatistics = new ImpalaSqlQuery {
    val sqlStatement = sqlCalculateStatisticsStatement
    val logMessage = sqlCalculateStatisticsLogMessage
  }

  override def sqlInsertTable(sqlQuery: ImpalaSqlQuery) = new ImpalaSqlQuery {
    val sqlStatement = sqlInsertTableStatement(sqlQuery.sqlStatement)
    val logMessage = sqlInsertTableLogMessage(sqlQuery.logMessage)
  }
  
  lazy val sqlInvalidateMetadataStatement = s"INVALIDATE METADATA ${tableName}"
  lazy val sqlInvalidateMetadataLogMessage = s"In Impala metadata on table ${tableName}"

  lazy val sqlInvalidateMetadata = new ImpalaSqlQuery {
    val sqlStatement = sqlInvalidateMetadataStatement
    val logMessage = sqlInvalidateMetadataLogMessage
  }

  lazy val sqlRefreshMetadataStatement = s"REFRESH ${tableName}"
  lazy val sqlRefreshMetadataLogMessage = s"Refreshing Impala metadata on table ${tableName}"

  lazy val sqlRefreshMetadata = new ImpalaSqlQuery {
    val sqlStatement = sqlRefreshMetadataStatement
    val logMessage = sqlRefreshMetadataLogMessage
  }
}
