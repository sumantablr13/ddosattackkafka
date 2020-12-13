package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import java.sql.SQLException

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.SparkExecutor
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.SupplierRecordsFile
import org.apache.spark.sql.DataFrame

/**
  * @author rdas2 on 9/24/2020
  *
  * */
class JdbcDatasource(appContext: AppContext
                     , sparkExecutor: SparkExecutor
                     , event:SupplierRecordsFile) {

  private val jdbcDatabase: String = appContext.config.getString("POSTGRES_JDBC_SCHEMA").trim

  private val pathVal = {
    val HDFS_NAME_NODE = appContext.config.getString("HDFS_NAME_NODE").trim
    val HDFS_USER_BASE = appContext.config.getString("HDFS_USER_BASE").trim
    val ENV_USER = appContext.config.getString("ENV_USER").trim
    val postgresPwd = appContext.config.getString("POSTGRES_JDBC_PASS").trim
    println(s"Jdbc path val: $HDFS_NAME_NODE$HDFS_USER_BASE/$ENV_USER/$postgresPwd")
    s"$HDFS_NAME_NODE$HDFS_USER_BASE/$ENV_USER/$postgresPwd"
  }

  private val jdbcPwd = sparkExecutor.file.getPwd(pathVal).trim
  private val jdbcUser = appContext.config.getString("POSTGRES_JDBC_USER").trim

  private val jdbcUrl = {
    val jdbcHostname = appContext.config.getString("POSTGRES_JDBC_HOST").trim
    val jdbcPort = appContext.config.getString("POSTGRES_JDBC_PORT").trim
    s"jdbc:postgresql://$jdbcHostname:$jdbcPort/$jdbcDatabase"
  }

  val fileChksumHist: String = appContext.config.getString("POSTGRES_DB_CHKSUM_HIST_TBL")

  val fileChksumOptions: Map[String, String] = Map[String, String](
    "jdbcUser" -> s"$jdbcUser",
    "jdbcPwd" -> s"$jdbcPwd",
    "jdbcTable" -> s"public.$fileChksumHist",
    "jdbcUrl" -> s"$jdbcUrl"
  )


    val fileChksumDF: DataFrame =
      try{
      sparkExecutor.sql.createJdbcSqlConnectionPostgres(fileChksumOptions)
        .select("checksumval","ts")
        .filter(s"corelationid == '${event.runId}'")
    }
  catch{
    case exception: Exception =>
      throw new SQLException(s"Error connecting with Postgres DB: $exception")
  }
}
