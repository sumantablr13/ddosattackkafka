package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{PartitionTable, Table}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

abstract class AbstractDdosAttackHysT(appContext: AppContext) extends PartitionTable(appContext.database) {

  override lazy val logicalName: String = "attack hystory"
  override lazy val physicalName: String = "ddos_attack"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("ipAddress", "STRING"),
    ("dateDs", "TIMESTAMP")/*,
    ("fileName", "STRING")*/
  )

 override lazy val partitionColumns: Array[(String, String)] = Array(
     ("fileName", "STRING")
   )

  override lazy val schema: StructType = (new StructType)
    .add("ipAddress", StringType)
    .add("dateDs", TimestampType)
    .add("fileName", StringType)



  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

   def helperCreatePartitionSpec(fileNameVal:String): String = s"msg',fileName='${fileNameVal}'"
}
