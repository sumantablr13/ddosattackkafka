package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.SparkSqlExecutor
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SparkSqlQuery
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset

import scala.util.Try

class KafkaOffsetPersistance(appContext: AppContext, sparkSqlExecutor: SparkSqlExecutor, table: MDataKafkaOffset) {

  def saveOffsetData(topicName: String, partition: Int, processCode: String, offset: Long): Unit = {
    val emptyDS = sparkSqlExecutor.emptyDatasetString
    import emptyDS.sparkSession.implicits._
    sparkSqlExecutor
      .insertTable(table,
        Seq((topicName, partition, processCode, offset+1, new TimestampConverter().unixTime))
        .toDF(
          "topic_nm",
          "prtn_nbr",
          "proc_cd",
          "offset",
          "isrted_ts",
          "isrted_by_usr_nm",
          "asset_cd"
        ), "","", topicName, new TimestampConverter())
    println(s"Wrote data: topic_nm:$topicName,|prtn_nbr:$partition|proc_cd:$processCode|offset:$offset to table ${table.tableName}...")
  }

  def readOffsetData(topicName: String, processCode: String): Long = {
    val emptyDS = sparkSqlExecutor.emptyDatasetString
    import emptyDS.sparkSession.implicits._

    println("Reading offset from table...")
    //Refreshing Kafka Table
    sparkSqlExecutor.refreshTable(table)
    val queryResult = sparkSqlExecutor.getDataFrame {
      new SparkSqlQuery {
        override val sqlStatement: String =s"""SELECT * FROM ${table.tableName} WHERE proc_cd = '$processCode' AND topic_nm = '$topicName' AND asset_cd = '' ORDER BY isrted_ts DESC LIMIT 1"""
        override val logMessage: String = "Get last persisted kafka offset."
      }
    }

    queryResult.show()

    Try {
      queryResult.map { row =>
        val topic = row.getAs[String]("topic_nm")
        val partition = row.getAs[Int]("prtn_nbr")
        val offset = row.getAs[Long]("offset")
        (topic, partition, offset)
      }.first()
    }.toOption match {
      case Some((topic, partition, offset)) =>
        println(s"Read and proceeding with latest successful offset: $offset")
        offset
      case None =>
        val msg = s"Couldn't read offset data... attempt of starting from offset: ${KafkaOffsetPersistance.earliest}"
        println(msg)
        throw new IllegalArgumentException("Kafka Offset Read Error: " + msg)
//        KafkaOffsetPersistance.earliest
    }
  }

}

object KafkaOffsetPersistance {
  val earliest = 0
}
