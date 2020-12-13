package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractRdsLocationCode(appContext: AppContext) extends Table("") {
  override lazy val logicalName: String = "RdsLocationCode"
  override lazy val physicalName: String = "v_dspnsd_rx_oac_brdg"

  override lazy val tableColumns: Array[(String, String)] = Array(
      ("panel_cd", "STRING")
    , ("panel_mbr_cd", "STRING")
    , ("supld_dspnsing_oac_id", "STRING")
    , ("dervd_dsupp_proc_id", "STRING")
    , ("sys_house_cd", "STRING")
    , ("sys_house_shrt_cd", "STRING")
    , ("sys_house_nm", "STRING")
    , ("panel_mbr_status_cd1", "STRING")
    , ("panel_mbr_status_nm1", "STRING")
    , ("panel_mbr_status_desc1", "STRING")
    , ("panel_mbr_status_cd2", "STRING")
    , ("panel_mbr_status_nm2", "STRING")
    , ("panel_mbr_status_desc2", "STRING")
    , ("dspnsing_oac_id", "STRING")
    , ("dspnsing_oac_lgcy_id", "STRING")
    , ("bus_eff_ts", "STRING")
    , ("bus_expry_ts", "STRING")
    , ("publ_eff_ts", "STRING")
    , ("publ_expry_ts", "STRING")
  )


  override lazy val schema: StructType = (new StructType)

    .add("panel_cd", StringType)
    .add("panel_mbr_cd", StringType)
    .add("supld_dspnsing_oac_id", StringType)
    .add("dervd_dsupp_proc_id", StringType)
    .add("sys_house_cd", StringType)
    .add("sys_house_shrt_cd", StringType)
    .add("sys_house_nm", StringType)
    .add("panel_mbr_status_cd1", StringType)
    .add("panel_mbr_status_nm1", StringType)
    .add("panel_mbr_status_desc1", StringType)
    .add("panel_mbr_status_cd2", StringType)
    .add("panel_mbr_status_nm2", StringType)
    .add("panel_mbr_status_desc2", StringType)
    .add("dspnsing_oac_id", StringType)
    .add("dspnsing_oac_lgcy_id", StringType)
    .add("bus_eff_ts" ,StringType)
    .add("bus_expry_ts" ,StringType)
    .add("publ_eff_ts" ,StringType)
    .add("publ_expry_ts" ,StringType)


  override lazy val createTableOpts: String =
    """
      STORED AS PARQUET
      TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """.stripMargin
}
