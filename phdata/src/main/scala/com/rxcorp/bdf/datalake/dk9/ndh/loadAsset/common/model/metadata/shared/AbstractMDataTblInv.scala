package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

abstract class AbstractMDataTblInv (appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "MetaDataTblInventory"
  override lazy val physicalName: String = "mdata_tbl_inv"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("logl_tbl_nm", "STRING"),
    ("physcl_tbl_nm", "STRING"),
    ("upstm_src_sys_cd", "STRING"),
    ("subj_area_nm", "STRING"),
    ("sub_subj_area_nm", "STRING"),
    ("tbl_desc", "STRING"),
    ("sys_eff_ts", "TIMESTAMP")
  )

  override lazy val schema: StructType = (new StructType)
    .add("logl_tbl_nm", StringType)
    .add("physcl_tbl_nm", StringType)
    .add("upstm_src_sys_cd", StringType)
    .add("subj_area_nm", StringType)
    .add("sub_subj_area_nm", StringType)
    .add("tbl_desc", StringType)
    .add("sys_eff_ts", TimestampType)

  override lazy val createTableOpts: String =
    """
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY '\001'
      |LINES TERMINATED BY '\n'
      |STORED AS TEXTFILE
    """.stripMargin


  def helperInsertStatement(
                             logl_tbl_nm: String,
                             physcl_tbl_nm: String,
                             upstm_src_sys_cd: String,
                             subj_area_nm: String,
                             sub_subj_area_nm: String,
                             tbl_desc: String,
                             sys_eff_ts: TimestampConverter
                           ): String = {
    s"""
       SELECT
         '${logl_tbl_nm}' AS logl_tbl_nm,
         '${physcl_tbl_nm}' AS physcl_tbl_nm,
         '${upstm_src_sys_cd}' AS upstm_src_sys_cd,
         '${subj_area_nm}' AS subj_area_nm,
         '${sub_subj_area_nm}' AS sub_subj_area_nm,
         '${tbl_desc}' AS tbl_desc,
          ${sys_eff_ts.sparkSqlUTCText} AS sys_eff_ts
     """
  }
}
