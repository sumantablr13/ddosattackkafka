package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractGeoLoc(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Geo Location"
  override lazy val physicalName: String = "v_geo_loc"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("BLD_NM", "STRING"),
    ("BUS_EFF_TS", "STRING"),
    ("BUS_EXPRY_TS", "STRING"),
    ("CITY_NM", "STRING"),
    ("CTRY_ISO_CD", "STRING"),
    ("DEL_STATUS_CD", "STRING"),
    ("FLR_NBR", "BIGINT"),
    ("GEO_LOC_CMPT_ID", "BIGINT"),
    ("GEO_LOC_X_COORD_NBR_TXT", "STRING"),
    ("GEO_LOC_Y_COORD_NBR_TXT", "STRING"),
    ("LOC_ADDR_LINE1_TXT", "STRING"),
    ("LOC_ADDR_LINE2_TXT", "STRING"),
    ("LOC_ADDR_LINE3_TXT", "STRING"),
    ("LOC_ADDR_LINE4_TXT", "STRING"),
    ("LOC_ID", "BIGINT"),
    ("POBOX_NBR", "STRING"),
    ("PSTL_CD_CD", "STRING"),
    ("PSTL_CD_FORMAT", "STRING"),
    ("RDM_INST_ID", "BIGINT"),
    ("SRC_CRTDT", "STRING"),
    ("SRC_NEW", "BOOLEAN"),
    ("SRC_SYS_CD", "STRING"),
    ("SRC_SYS_KEY", "STRING"),
    ("SRC_TXNCD", "STRING"),
    ("SRC_TXNDT", "STRING"),
    ("STR_NBR_TXT", "STRING"),
    ("SUITE_NM", "STRING"),
    ("VERSION_ID", "BIGINT"),
    ("PUBL_EFF_TS", "STRING"),
    ("PUBL_EXPRY_TS", "STRING")
  )

  override lazy val schema: StructType = (new StructType)
    .add("bld_nm", StringType)
    .add("bus_eff_ts", StringType)
    .add("bus_expry_ts", StringType)
    .add("city_nm", StringType)
    .add("ctry_iso_cd", StringType)
    .add("del_status_cd", StringType)
    .add("flr_nbr", LongType)
    .add("geo_loc_cmpt_id", LongType)
    .add("geo_loc_x_coord_nbr_txt", StringType)
    .add("geo_loc_y_coord_nbr_txt", StringType)
    .add("loc_addr_line1_txt", StringType)
    .add("loc_addr_line2_txt", StringType)
    .add("loc_addr_line3_txt", StringType)
    .add("loc_addr_line4_txt", StringType)
    .add("loc_id", LongType)
    .add("pobox_nbr", StringType)
    .add("pstl_cd_cd", StringType)
    .add("pstl_cd_format", StringType)
    .add("rdm_inst_id", LongType)
    .add("src_crtdt", StringType)
    .add("src_new", BooleanType)
    .add("src_sys_cd", StringType)
    .add("src_sys_key", StringType)
    .add("src_txncd", StringType)
    .add("src_txndt", StringType)
    .add("str_nbr_txt", StringType)
    .add("suite_nm", StringType)
    .add("version_id", LongType)
    .add("publ_eff_ts", StringType)
    .add("publ_expry_ts", StringType)

  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

}
