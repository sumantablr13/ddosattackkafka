package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{PartitionTable, Table}
import org.apache.spark.sql.types._

abstract class AbstractMDataKafkaOffset(appContext: AppContext) extends Table(appContext.database) {

  override lazy val logicalName: String = "Metadata Kafka Offset"
  override lazy val physicalName: String = "mdata_kafka_offset"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("topic_nm", "STRING"),
    ("prtn_nbr", "INT"),
    ("proc_cd", "STRING"),
    ("offset", "BIGINT"),
    ("isrted_ts", "TIMESTAMP")
  )



  override lazy val schema: StructType = (new StructType)
    .add("topic_nm", StringType)
    .add("prtn_nbr", IntegerType)
    .add("proc_cd", StringType)
    .add("offset", LongType)
    .add("isrted_ts", TimestampType)


  override lazy val createTableOpts: String =
    """
      |STORED AS SEQUENCEFILE
      |TBLPROPERTIES ('COLUMN_STATS_ACCURATE'='true', 'auto.purge'='true')
    """.stripMargin
}

