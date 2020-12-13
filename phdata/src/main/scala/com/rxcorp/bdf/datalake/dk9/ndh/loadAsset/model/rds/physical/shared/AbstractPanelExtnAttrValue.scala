package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractPanelExtnAttrValue(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Panel Member External Value"
  override lazy val physicalName: String = "v_panel_extn_attr_value"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("attr_grp_nm", "STRING"),
    ("attr_nm", "STRING"),
    ("attr_value_txt", "STRING"),
    ("bus_eff_ts", "STRING"),
    ("bus_expry_ts", "STRING"),
    ("del_status_cd", "STRING"),
    ("ltrl_value_txt", "STRING"),
    ("panel_cd", "STRING"),
    ("panel_mbr_seq_nbr", "BIGINT"),
    ("rdm_inst_id", "BIGINT"),
    ("src_crtdt", "STRING"),
    ("src_new", "BOOLEAN"),
    ("src_sys_cd", "STRING"),
    ("src_sys_key", "STRING"),
    ("src_txncd", "STRING"),
    ("src_txndt", "STRING"),
    ("version_id", "BIGINT"),
    ("publ_eff_ts", "STRING"),
    ("publ_expry_ts", "STRING")
  )

	override lazy val schema: StructType =
    (new StructType)
      .add("attr_grp_nm", StringType)
      .add("attr_nm", StringType)
      .add("attr_value_txt", StringType)
      .add("bus_eff_ts", StringType)
      .add("bus_expry_ts", StringType)
      .add("del_status_cd", StringType)
      .add("ltrl_value_txt", StringType)
      .add("panel_cd", StringType)
      .add("panel_mbr_seq_nbr", LongType)
      .add("rdm_inst_id", LongType)
      .add("src_crtdt", StringType)
      .add("src_new", BooleanType)
      .add("src_sys_cd", StringType)
      .add("src_sys_key", StringType)
      .add("src_txncd", StringType)
      .add("src_txndt", StringType)
      .add("version_id", LongType)
      .add("publ_eff_ts", StringType)
      .add("publ_expry_ts", StringType)

	override lazy val createTableOpts =
		s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

}
