package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractOac(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Operational Activity Center"
  override lazy val physicalName: String = "v_oac"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("ACTVTY_CTR_NM","STRING"),
	("BUS_EFF_TS","STRING"),
	("BUS_EXPRY_TS","STRING"),
	("DEL_STATUS_CD","STRING"),
	("LOC_ID","BIGINT"),
	("OAC_DESC","STRING"),
	("OAC_DISPENSES_MEDCN_IND","BOOLEAN"),
	("OAC_FCN_SPECLSM_TYP_CD","STRING"),
	("OAC_HOSP_NATL_ADM_CD","STRING"),
	("OAC_LNCH_DT","STRING"),
	("OAC_MAIN_FAX_NBR_TXT","STRING"),
	("OAC_MAIN_PHONE_NBR_TXT","STRING"),
	("OAC_NATL_ADM_CD","STRING"),
	("OAC_NBR_OF_BEDS_CNT","BIGINT"),
	("OAC_NM","STRING"),
	("OAC_OPENING_HHS_TXT","STRING"),
	("OAC_ORG_TYP_CD","STRING"),
	("OAC_SHORT_NM","STRING"),
	("OAC_STATUS_CD","STRING"),
	("OAC_STATUS_DT","STRING"),
	("OAC_STATUS_RSN_CD","STRING"),
	("OAC_ST_CD","STRING"),
	("OAC_TRNG_IND","BOOLEAN"),
	("ORG_ID","BIGINT"),
	("ORG_OPUNIT_NM","STRING"),
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
	.add("actvty_ctr_nm",StringType)
	.add("bus_eff_ts",StringType)
	.add("bus_expry_ts",StringType)
	.add("del_status_cd",StringType)
	.add("loc_id",LongType)
	.add("oac_desc",StringType)
	.add("oac_dispenses_medcn_ind",BooleanType)
	.add("oac_fcn_speclsm_typ_cd",StringType)
	.add("oac_hosp_natl_adm_cd",StringType)
	.add("oac_lnch_dt",StringType)
	.add("oac_main_fax_nbr_txt",StringType)
	.add("oac_main_phone_nbr_txt",StringType)
	.add("oac_natl_adm_cd",StringType)
	.add("oac_nbr_of_beds_cnt",LongType)
	.add("oac_nm",StringType)
	.add("oac_opening_hhs_txt",StringType)
	.add("oac_org_typ_cd",StringType)
	.add("oac_short_nm",StringType)
	.add("oac_status_cd",StringType)
	.add("oac_status_dt",StringType)
	.add("oac_status_rsn_cd",StringType)
	.add("oac_st_cd",StringType)
	.add("oac_trng_ind",BooleanType)
	.add("org_id",LongType)
	.add("org_opunit_nm",StringType)
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
