package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractPanelMbr(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Panel Member"
  override lazy val physicalName: String = "v_panel_mbr"

  override lazy val tableColumns: Array[(String, String)] = Array(
		("actvty_ctr_nm", "STRING"),
		("bus_eff_ts", "STRING"),
		("bus_expry_ts", "STRING"),
		("del_status_cd", "STRING"),
		("loc_id", "BIGINT"),
		("org_id", "BIGINT"),
		("org_opunit_nm", "STRING"),
		("panel_cd", "STRING"),
		("panel_mbr_cd", "STRING"),
		("panel_mbr_eff_dt", "STRING"),
		("panel_mbr_expry_dt", "STRING"),
		("panel_mbr_rpt_nbr", "BIGINT"),
		("panel_mbr_seq_nbr", "BIGINT"),
		("panel_mbr_status_cd", "STRING"),
		("panel_mbr_status_dt", "STRING"),
		("pers_id", "BIGINT"),
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
			.add("actvty_ctr_nm", StringType)
			.add("bus_eff_ts", StringType)
			.add("bus_expry_ts", StringType)
			.add("del_status_cd", StringType)
			.add("loc_id", LongType)
			.add("org_id", LongType)
			.add("org_opunit_nm", StringType)
			.add("panel_cd", StringType)
			.add("panel_mbr_cd", StringType)
			.add("panel_mbr_eff_dt", StringType)
			.add("panel_mbr_expry_dt", StringType)
			.add("panel_mbr_rpt_nbr", LongType)
			.add("panel_mbr_seq_nbr", LongType)
			.add("panel_mbr_status_cd", StringType)
			.add("panel_mbr_status_dt", StringType)
			.add("pers_id", LongType)
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
