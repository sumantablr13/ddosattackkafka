package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractOacAssc(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Operational Activity Center"
  override lazy val physicalName: String = "v_oac_assc"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("BUS_EFF_TS", "STRING"),
    ("BUS_EXPRY_TS", "STRING"),
    ("CHILD_ACTVTY_CTR_NM", "STRING"),
    ("CHILD_LOC_ID", "BIGINT"),
    ("CHILD_ORG_ID", "BIGINT"),
    ("CHILD_ORG_OPUNIT_NM", "STRING"),
    ("DEL_STATUS_CD", "STRING"),
    ("OAC_ASSC_EFF_DT", "STRING"),
    ("OAC_ASSC_EXPRY_DT", "STRING"),
    ("OAC_ASSC_TYP_CD", "STRING"),
    ("PAR_ACTVTY_CTR_NM", "STRING"),
    ("PAR_LOC_ID", "BIGINT"),
    ("PAR_ORG_ID", "BIGINT"),
    ("PAR_ORG_OPUNIT_NM", "STRING"),
    ("RDM_INST_ID", "BIGINT"),
    ("SRC_CRTDT", "STRING"),
    ("SRC_NEW", "BOOLEAN"),
    ("SRC_SYS_CD", "STRING"),
    ("SRC_SYS_KEY", "STRING"),
    ("SRC_TXNCD", "STRING"),
    ("SRC_TXNDT", "STRING"),
    ("VERSION_ID", "BIGINT"),
    ("PUBL_EFF_TS", "STRING"),
    ("PUBL_EXPRY_TS", "STRING")
  )

  override lazy val schema: StructType = (new StructType)
    .add("bus_eff_ts", StringType)
    .add("bus_expry_ts", StringType)
    .add("child_actvty_ctr_nm", StringType)
    .add("child_loc_id", LongType)
    .add("child_org_id", LongType)
    .add("child_org_opunit_nm", StringType)
    .add("del_status_cd", StringType)
    .add("oac_assc_eff_dt", StringType)
    .add("oac_assc_expry_dt", StringType)
    .add("oac_assc_typ_cd", StringType)
    .add("par_actvty_ctr_nm", StringType)
    .add("par_loc_id", LongType)
    .add("par_org_id", LongType)
    .add("par_org_opunit_nm", StringType)
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
