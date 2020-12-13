package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.TimestampConverter
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{PartitionTable, SparkPartitionTable, Table}
import org.apache.spark.sql.types._

abstract class AbstractMDataTblLoadLog(database: String) extends PartitionTable(database) {

  override lazy val logicalName: String = "Metadata Table Load Log"
  override lazy val physicalName: String = "mdata_tbl_load_log"

  override lazy val tableColumns = Array(
    ("logl_tbl_nm","STRING"),
    ("data_contxt_cd","STRING"),
    ("appl_id","STRING"),
    ("nom_bus_eff_ts","TIMESTAMP"),
    ("nom_bus_expry_ts","TIMESTAMP"),
    ("src_publ_ts","TIMESTAMP"),
    ("sys_eff_ts","TIMESTAMP")
  )

  override lazy val partitionColumns = Array(
    ("asset_cd","STRING")
  )

  override lazy val schema = (new StructType)
    .add("logl_tbl_nm", StringType)
    .add("data_contxt_cd", StringType)
    .add("appl_id",StringType)
    .add("nom_bus_eff_ts",TimestampType)
    .add("nom_bus_expry_ts",TimestampType)
    .add("src_publ_ts",TimestampType)
    .add("sys_eff_ts",TimestampType)
    .add("asset_cd",LongType)

  override lazy val createTableOpts =
    """
       | ROW FORMAT DELIMITED
       | FIELDS TERMINATED BY '\001'
       | LINES TERMINATED BY '\n'
       | STORED AS TEXTFILE
    """.stripMargin

  def helperCreatePartitionSpec(assetCd: String): String = s"asset_cd=CAST('${assetCd}' AS STRING)"
  def helperInsertStatement(
                             table: Table,
                             dataContextCode: String,
                             applicationId: String,
                             nominalBusEffTs: TimestampConverter,
                             nominalBusExpryTs: TimestampConverter,
                             sourcePublTs: TimestampConverter,
                             assetCode: String
                           ): String = {
    s"""
        SELECT
          '${table.logicalName}' AS logl_tbl_nm,
          '${dataContextCode}' AS data_contxt_cd,
          '${applicationId}' AS appl_id,
          ${nominalBusEffTs.sparkSqlUTCText} AS nom_bus_eff_ts,
          ${nominalBusExpryTs.sparkSqlUTCText} AS nom_bus_expry_ts,
          ${sourcePublTs.sparkSqlUTCText} AS src_publ_ts,
          ${new TimestampConverter().sparkSqlUTCText} AS sys_eff_ts,
          '${assetCode}' AS asset_cd
     """
  }
}
