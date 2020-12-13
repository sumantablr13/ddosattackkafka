package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractOacExtnAttrVal(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Operational Activity Center Extension Attribute Value"
  override lazy val physicalName: String = "v_oac_extn_attr_value"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("actvty_ctr_nm","STRING"),//oac nm
	("attr_grp_nm","STRING"),
	("attr_nm","STRING"),
	("attr_value_txt","STRING"),
	("bus_eff_ts","STRING"),
	("bus_expry_ts","STRING"),
	("del_status_cd","STRING"),
	("loc_id","INT"),
	("ltrl_value_txt","STRING"),//lgcy oac
	("org_id","INT"),
	("org_opunit_nm","STRING"),
	("rdm_inst_id","INT"),
	("src_crtdt","STRING"),
	("src_new","BOOLEAN"),
	("src_sys_cd","STRING"),
	("src_sys_key","STRING"),
	("src_txncd","STRING"),
	("src_txndt","STRING"),
	("version_id","INT"),
	("publ_eff_ts","STRING"),
	("publ_expry_ts","STRING")
  )

	override lazy val schema: StructType =
    (new StructType)
	.add("actvty_ctr_nm",StringType)
	.add("attr_grp_nm",StringType)
	.add("attr_nm",StringType)
	.add("attr_value_txt",StringType)
	.add("bus_eff_ts",StringType)
	.add("bus_expry_ts",StringType)
	.add("del_status_cd",StringType)
	.add("loc_id",IntegerType)
	.add("ltrl_value_txt",StringType)
	.add("org_id",IntegerType)
	.add("org_opunit_nm",StringType)
	.add("rdm_inst_id",IntegerType)
	.add("src_crtdt",StringType)
	.add("src_new",BooleanType)
	.add("src_sys_cd",StringType)
	.add("src_sys_key",StringType)
	.add("src_txncd",StringType)
	.add("src_txndt",StringType)
	.add("version_id",IntegerType)
	.add("publ_eff_ts",StringType)
	.add("publ_expry_ts",StringType)

	override lazy val createTableOpts =
	//    ROW FORMAT DELIMITED
	//    FIELDS TERMINATED BY '\\001'
	//    LINES TERMINATED BY '\\n'
		s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

}
