package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractBndryAssc(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Boundary Association"
  override lazy val physicalName: String = "v_bndry_assc"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("BNDRY_ASSC_DESC","STRING"),
	("BNDRY_ASSC_SCHEM_CD","STRING"),
	("BNDRY_ASSC_TYP_CD","STRING"),
	("BUS_EFF_TS","STRING"),
	("BUS_EXPRY_TS","STRING"),
	("CHILD_BNDRY_ASSC_ID","BIGINT"),
	("DEL_STATUS_CD","STRING"),
	("PAR_BNDRY_ASSC_ID","BIGINT"),
	("RDM_INST_ID","BIGINT"),
	("SRC_CRTDT","STRING"),
	("SRC_NEW","BOOLEAN"),
	("SRC_SYS_CD","STRING"),
	("SRC_SYS_KEY","STRING"),
	("SRC_TXNCD","STRING"),
	("SRC_TXNDT","STRING"),
	("VERSION_ID","BIGINT"),
	("PUBL_EFF_TS","STRING"),
	("PUBL_EXPRY_TS","STRING")
  )

	override lazy val schema: StructType =
    (new StructType)
	.add("bndry_assc_desc",StringType)
	.add("bndry_assc_schem_cd",StringType)
	.add("bndry_assc_typ_cd",StringType)
	.add("bus_eff_ts",StringType)
	.add("bus_expry_ts",StringType)
	.add("child_bndry_assc_id",LongType)
	.add("del_status_cd",StringType)
	.add("par_bndry_assc_id",LongType)
	.add("rdm_inst_id",LongType)
	.add("src_crtdt",StringType)
	.add("src_new",BooleanType)
	.add("src_sys_cd",StringType)
	.add("src_sys_key",StringType)
	.add("src_txncd",StringType)
	.add("src_txndt",StringType)
	.add("version_id",LongType)
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
