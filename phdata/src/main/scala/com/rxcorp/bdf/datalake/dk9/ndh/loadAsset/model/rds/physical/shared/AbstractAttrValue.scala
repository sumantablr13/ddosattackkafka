package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractAttrValue(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Attribute Value"
  override lazy val physicalName: String = "v_attr_value"

  override lazy val tableColumns: Array[(String, String)] = Array(
		("attr_nm", "STRING"),
		("attr_value_cmt_txt", "STRING"),
		("attr_value_desc", "STRING"),
		("attr_value_nm", "STRING"),
		("attr_value_short_nm", "STRING"),
		("attr_value_txt", "STRING"),
		("bus_eff_ts", "STRING"),
		("bus_expry_ts", "STRING"),
		("conforming_clas_cd", "STRING"),
		("del_status_cd", "STRING"),
		("dmn_customized_node_value_ind", "STRING"),
		("lang_cd", "STRING"),
		("lang_char_set_cd", "STRING"),
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
			.add("attr_nm", StringType)
			.add("attr_value_cmt_txt", StringType)
			.add("attr_value_desc", StringType)
			.add("attr_value_nm", StringType)
			.add("attr_value_short_nm", StringType)
			.add("attr_value_txt", StringType)
			.add("bus_eff_ts", StringType)
			.add("bus_expry_ts", StringType)
			.add("conforming_clas_cd", StringType)
			.add("del_status_cd", StringType)
			.add("dmn_customized_node_value_ind", StringType)
			.add("lang_cd", StringType)
			.add("lang_char_set_cd", StringType)
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
