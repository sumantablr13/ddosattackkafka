package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SparkSqlQuery

trait SparkTable extends Table {
  override type T_SqlType = SparkSqlQuery

  override lazy val sqlCreateTable = new SparkSqlQuery {
    val sqlStatement = sqlCreateTableStatement
    val logMessage = sqlCreateTableLogMessage
  }

  override lazy val sqlDropTable = new SparkSqlQuery {
    val sqlStatement = sqlDropTableStatement
    val logMessage = sqlDropTableLogMessage
  }

  override lazy val sqlTruncateTable = new SparkSqlQuery {
    val sqlStatement = sqlTruncateTableStatement
    val logMessage = sqlTruncateTableLogMessage
  }

  override def sqlCalculateStatistics = new SparkSqlQuery {
    val sqlStatement = sqlCalculateStatisticsStatement
    val logMessage = sqlCalculateStatisticsLogMessage
  }

  override def sqlInsertTable(sqlQuery: SparkSqlQuery) = new SparkSqlQuery {
    val sqlStatement = sqlInsertTableStatement(sqlQuery.sqlStatement)
    val logMessage = sqlInsertTableLogMessage(sqlQuery.logMessage)
  }
}
