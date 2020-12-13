package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, TimestampConverter}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

abstract class AbstractMDataTblSignOffLog(appContext: AppContext) extends PartitionTable("") {

  override lazy val logicalName: String = "Metadata Table Sign Off Log"
  override lazy val physicalName: String = "mdata_tbl_sign_off_log"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("prd_cyc_id", "STRING"),
    ("subj_area_nm", "STRING"),
    ("sub_subj_area_nm", "STRING"),
    ("assct_src_publ_ts", "TIMESTAMP"),
    ("data_cntx_cd", "STRING"),
    ("sys_eff_ts", "TIMESTAMP"),
    ("sys_expry_ts", "TIMESTAMP")
  )

  override lazy val partitionColumns: Array[(String, String)] = Array(
    ("asset_cd", "STRING")
  )

  override lazy val schema: StructType = (new StructType)
    .add("prd_cyc_id", StringType)
    .add("subj_area_nm", StringType)
    .add("sub_subj_area_nm", StringType)
    .add("assct_src_publ_ts", TimestampType)
    .add("data_cntx_cd", StringType)
    .add("sys_eff_ts", TimestampType)
    .add("sys_expry_ts", TimestampType)
    .add("asset_cd", StringType)

  override lazy val createTableOpts: String =
    """
     |ROW FORMAT DELIMITED
     |FIELDS TERMINATED BY '\001'
     |LINES TERMINATED BY '\n'
     |STORED AS TEXTFILE
    """.stripMargin

  def helperCreatePartitionSpec(assetCd: String): String = s"asset_cd='${assetCd}'"

  def helperInsertStatement(
                             assetCode: String,
                             periodLevelCode: String,
                             PeriodId: Int,
                             subjAreaName: String,
                             subSubjAreaName: String,
                             assctSrcPublTs: TimestampConverter,
                             dataCntxCd: String,
                             sys_eff_ts : TimestampConverter
                           ): String = {
    s"""
        SELECT
          '${assetCode}_${periodLevelCode}_${PeriodId}' AS prd_cyc_id,
          '${subjAreaName}' AS subj_area_nm,
          '${subSubjAreaName}' AS sub_subj_area_nm,
           ${assctSrcPublTs.sparkSqlUTCText} AS assct_src_publ_ts,
          '${dataCntxCd}' AS data_cntx_cd,
           ${sys_eff_ts.sparkSqlUTCText} AS sys_eff_ts,
           ${TimestampConverter.TIMESTAMP_PLUS_INFINITY.sparkSqlUTCText} AS sys_expry_ts,
          '${assetCode}' AS asset_cd
     """

  }


}
