package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataTblLoadLog
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{SparkPartitionTable, SparkTable}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SparkSqlQuery
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{ TimestampConverter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.broadcast

import scala.reflect.runtime.universe._

class SparkSqlExecutor(private val spark: SparkSession) extends SqlExecutor {
  override val executorName: String = "Spark SQL Executor"

  type T_SqlQueryType = SparkSqlQuery
  type T_TableType = SparkTable
  type T_PartitionTableType = SparkPartitionTable

  final override protected def getDataFrameOnly(sqlStatement: String): DataFrame = {
    try {
      spark.sql(sqlStatement)

    }
    catch {
      case e: QueryExecutionException =>
       // logger.logError(e)

        spark.sparkContext.stop()
        throw e
    }
  }

  def getDfAndViewWithTempViewCreated(sqlQuery: SparkSqlQuery,
                                      viewName: String = "NONE",
                                      storageLevel: StorageLevel = StorageLevel.NONE,
                                      enableBroadcast: Boolean = false): (DataFrame, String) = {
    val dfs = getDataFrame(sqlQuery)
    val df = if (enableBroadcast) broadcast(dfs) else dfs
    if (!storageLevel.equals(StorageLevel.NONE)) df.persist(storageLevel)
    if (!viewName.equals("NONE")) df.createOrReplaceTempView(viewName)

    (df, viewName)
  }

  def createTempViewForSql(sqlQuery: SparkSqlQuery,
                           viewName: String,
                           storageLevel: StorageLevel = StorageLevel.NONE,
                           enableBroadcast: Boolean = false): String = {
    val (df, retViewName) = getDfAndViewWithTempViewCreated(sqlQuery, viewName, storageLevel, enableBroadcast)
    viewName
  }

  def getDfWithTempViewCreated(sqlQuery: SparkSqlQuery,
                              viewName: String = "NONE",
                              storageLevel: StorageLevel = StorageLevel.NONE,
                              enableBroadcast: Boolean = false): DataFrame = {
    val (df, retViewName) = getDfAndViewWithTempViewCreated(sqlQuery, viewName, storageLevel, enableBroadcast)
    df
  }

  final override protected def executeSqlOnly(sqlStatement: String): Boolean = {
    getDataFrameOnly(sqlStatement)
    true
  }

  final override protected def prepareQueryNoResults(sqlQuery: SparkSqlQuery): SparkSqlQuery = {
    new SparkSqlQuery {
      val sqlStatement = s"${sqlQuery.sqlStatement} LIMIT 0"
      val logMessage = s"Trying SQL with 0 rows returned --> ${sqlQuery.logMessage}"
    }
  }

  final override protected def logSql(sqlQuery: SparkSqlQuery): Boolean = {
    //logger.logMessage(sqlQuery.logMessage, sqlQuery.sqlStatement)
    true
  }

    // This is the Spark implementation of refreshTable execution
    def refreshTable(table: SparkTable): Boolean = {
      // logger.logMessage(s"Refreshing metadata for table: ${table.tableName}")
      try {
        spark.catalog.refreshTable(table.tableName)
      }
      catch {
        case e: QueryExecutionException =>
          //logger.logError(e)

          spark.sparkContext.stop()
          throw e
      }
      true
    }


  // This is the Spark implementation of createTable execution
  override def createTable(table: SparkTable): Boolean = executeSql(table.sqlCreateTable)

  // This is the Spark implementation of dropTable executionTable.scala
  override def dropTable(table: SparkTable): Boolean = executeSql(table.sqlDropTable)

  // This is the Spark implementation of truncateTable execution
  override def truncateTable(table: SparkTable): Boolean = executeSql(table.sqlTruncateTable)

  // This is the Spark implementation of insertTable execution
  def insertTable(
                   table: SparkTable,
                   sqlQuery: SparkSqlQuery,
                   assetCode: String,
                   dataContextCode: String,
                   sourcePublTs: TimestampConverter
                 ): Boolean = insertTable(
    table, sqlQuery, assetCode, dataContextCode, sourcePublTs,
    TimestampConverter.TIMESTAMP_MINUS_INFINITY,
    TimestampConverter.TIMESTAMP_PLUS_INFINITY
  )

  override def insertTable(
                            table: SparkTable,
                            sqlQuery: SparkSqlQuery,
                            assetCode: String,
                            dataContextCode: String,
                            sourcePublTs: TimestampConverter,
                            nominalBusEffTs: TimestampConverter,
                            nominalBusExpryTs: TimestampConverter
                          ): Boolean = {
    executeSql(table.sqlInsertTable(sqlQuery))
    addMetadataTableLoadLog(table, assetCode, dataContextCode, sourcePublTs, nominalBusEffTs, nominalBusExpryTs)
  }

  // This is the Spark implementation of insertOverwriteTable execution
  def insertOverwriteTable(
                            table: SparkPartitionTable,
                            sqlQuery: SparkSqlQuery,
                            assetCode: String,
                            dataContextCode: String,
                            sourcePublTs: TimestampConverter
                          ): Boolean = {
    insertOverwriteTable(
      table, sqlQuery, assetCode, dataContextCode, sourcePublTs,
      TimestampConverter.TIMESTAMP_MINUS_INFINITY,
      TimestampConverter.TIMESTAMP_PLUS_INFINITY
    )
  }

  override def insertOverwriteTable(
                                     table: SparkPartitionTable,
                                     sqlQuery: SparkSqlQuery,
                                     assetCode: String,
                                     dataContextCode: String,
                                     sourcePublTs: TimestampConverter,
                                     nominalBusEffTs: TimestampConverter,
                                     nominalBusExpryTs: TimestampConverter
                                   ): Boolean = {
    executeSql(table.sqlInsertOverwriteTable(sqlQuery))
    addMetadataTableLoadLog(table, assetCode, dataContextCode, sourcePublTs, nominalBusEffTs, nominalBusExpryTs)
  }

  // This is the Spark implementation of dropPartition execution
  override def dropPartition(table: SparkPartitionTable, partitionSpec: String): Boolean =
    executeSql(table.sqlDropPartition(partitionSpec))

  // This is the Spark implementation of renamePartition execution
  override def renamePartition(table: SparkPartitionTable, oldPartitionSpec: String, newPartitionSpec: String): Boolean =
    executeSql(table.sqlRenamePartition(oldPartitionSpec, newPartitionSpec))

  // This is the Spark implementation of calculateStatistics execution
  override def calculateStatistics(table: SparkTable): Boolean =
    executeSql(table.sqlCalculateStatistics)

  // This is the Spark implementation of calculateStatistics execution for partition table
  override def calculateStatistics(table: SparkPartitionTable, partitionSpec: String): Boolean =
    executeSql(table.sqlCalculateStatistics(partitionSpec))

  def registerAsTempTable(df: DataFrame, dfDescription: String, viewName: String): Boolean = {
    //logger.logMessage(s"Register $dfDescription as temporary table $viewName")
    df.createOrReplaceTempView(viewName)
    true
  }

  def getMetadataTableLoadLog(databaseName: String): MDataTblLoadLog = new MDataTblLoadLog(databaseName)

  override protected def addMetadataTableLoadLog(
                                                  table: SparkTable,
                                                  assetCode: String,
                                                  dataContextCode: String,
                                                  sourcePublTs: TimestampConverter,
                                                  nominalBusEffTs: TimestampConverter,
                                                  nominalBusExpryTs: TimestampConverter
                                                ): Boolean = {
    val mDataTblLoadLog = getMetadataTableLoadLog(table.databaseName.toLowerCase.replaceAll("_ods", ""))
    val sqlQuery = new SparkSqlQuery {
      override val sqlStatement: String = mDataTblLoadLog.helperInsertStatement(table, dataContextCode, spark.sparkContext.applicationId, nominalBusEffTs, nominalBusExpryTs, sourcePublTs, assetCode)
      override val logMessage: String = s"Logging load operation in metadata for table ${table.logicalName}(${table.tableName})"
    }
    executeSql(mDataTblLoadLog.sqlInsertTable(sqlQuery))
  }

  def insertTable(
                   table: SparkTable,
                   df: DataFrame,
                   dfDescription: String,
                   assetCode: String,
                   dataContextCode: String,
                   sourcePublTs: TimestampConverter
                 ): Boolean = {
    insertTable(
      table, df, dfDescription, assetCode, dataContextCode, sourcePublTs,
      TimestampConverter.TIMESTAMP_MINUS_INFINITY,
      TimestampConverter.TIMESTAMP_PLUS_INFINITY
    )
  }

  def insertTable(
                   table: SparkTable,
                   df: DataFrame,
                   dfDescription: String,
                   assetCode: String,
                   dataContextCode: String,
                   sourcePublTs: TimestampConverter,
                   nominalBusEffTs: TimestampConverter,
                   nominalBusExpryTs: TimestampConverter
                 ): Boolean = {
    println("")
    val orderedDf = df.select(table.schema.map(field => col(field.name)): _*)
    //logger.logMessage(table.sqlInsertTableLogMessage(dfDescription))
    orderedDf.write.mode("append").insertInto(table.tableName)
    //addMetadataTableLoadLog(table, assetCode, dataContextCode, sourcePublTs, nominalBusEffTs, nominalBusExpryTs)
    true
  }

  def insertOverwriteTable(
                            table: SparkTable,
                            df: DataFrame,
                            dfDescription: String,
                            assetCode: String,
                            dataContextCode: String,
                            sourcePublTs: TimestampConverter
                          ): Boolean = {
    insertOverwriteTable(
      table, df, dfDescription, assetCode, dataContextCode, sourcePublTs,
      TimestampConverter.TIMESTAMP_MINUS_INFINITY,
      TimestampConverter.TIMESTAMP_PLUS_INFINITY
    )
  }

  def createJdbcSqlConnection(options: Map[String, String]): DataFrame = {
    try {
      spark.read
        .format("jdbc")
        .option("url", options("jdbcUrl").toString)
        .option("user", options("jdbcUser").toString)
        .option("password", options("jdbcPwd").toString)
        .option("dbtable", s"(${options("jdbcSql").toString}) x")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    }catch {
        case e:Exception=>println("")
        throw e
    }
  }

  def createJdbcSqlConnectionPostgres(options: Map[String, String]): DataFrame = {
    try {
      spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", options("jdbcUrl"))
        .option("user", options("jdbcUser"))
        .option("password", options("jdbcPwd"))
        .option("dbtable", options("jdbcTable"))
        .load()
    }catch {
       case e:Exception=>println("")
        throw e
    }
  }

  def writeJdbcSqlTable(outDf:DataFrame,options: Map[String, String]) = {
    outDf.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", options("jdbcUrl").toString)
      .option("user", options("jdbcUser").toString)
      .option("password", options("jdbcPwd").toString)
      .option("dbtable", options("jdbcSql").toString)
      .option("createTableColumnTypes",options("createTableColumnTypes").toString)
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .save()
  }

  def insertOverwriteTable(
                            table: SparkTable,
                            df: DataFrame,
                            dfDescription: String,
                            assetCode: String,
                            dataContextCode: String,
                            sourcePublTs: TimestampConverter,
                            nominalBusEffTs: TimestampConverter,
                            nominalBusExpryTs: TimestampConverter
                          ): Boolean = {
    val orderedDf = df.select(table.schema.map(field => col(field.name)): _*)
    table match {
      case table: SparkPartitionTable =>
       // logger.logMessage(table.sqlInsertOverwriteTableLogMessage(dfDescription))
        orderedDf.write.mode(SaveMode.Overwrite).insertInto(table.tableName)
      case table: SparkTable =>
       // logger.logMessage(table.sqlInsertTableLogMessage(dfDescription))
        orderedDf.write.mode(SaveMode.Overwrite).insertInto(table.tableName)
    }
    addMetadataTableLoadLog(table, assetCode, dataContextCode, sourcePublTs, nominalBusEffTs, nominalBusExpryTs)
    true
  }

  def emptyDatasetString: Dataset[String] = {
    import spark.implicits._
    spark.emptyDataset[String]
  }

  def emptyDataSet[T <: Product : TypeTag]: Dataset[T] = {
    import spark.implicits._
    spark.emptyDataset[T]
  }

  def getTable(table: SparkTable): DataFrame = {
    //logger.logMessage(s"Collect data from ${table.tableName} as Dataset / DataFrame")
    spark.table(table.tableName)
  }

  def executeBridgingQuery(sql:String):DataFrame={
    spark.sql(sql)
  }
  def createDataFrame(rdd: RDD[Row], rddDesc: String, schema: StructType): DataFrame = {
    //logger.logMessage(s"Re-create DataFrame '$rddDesc' skipping lineage")
    spark.createDataFrame(rdd, schema)
  }
}
