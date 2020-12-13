package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types._

/**
  * shiddesha.mydur created on 12/08/2019
  */
abstract class AbstractDsuppDspnsdRxHdr(appContext: AppContext) extends PartitionTable("") {

  override lazy val logicalName: String = "Data Supplier Dispensed Prescription Header"
  override lazy val physicalName: String = "dsupp_dspnsd_rx_hdr"

  override lazy val tableColumns = Array(
      ("file_cre_dt", "STRING")
    , ("file_id", "STRING")
    , ("dspnsing_oac_id", "STRING")
    , ("dspnsing_oac_nm", "STRING")
    , ("dspnsing_oac_addr_txt", "STRING")
    , ("dspnsing_oac_pstl_cd_cd", "STRING")
    , ("cff_data_strc_def_nm","STRING")
    , ("supld_data_strc_def_nm", "STRING")
    , ("bus_eff_dt", "TIMESTAMP")
    , ("publ_eff_ts", "TIMESTAMP")
    , ("proc_eff_ts", "TIMESTAMP")
    , ("src_sys_cre_ts", "TIMESTAMP")
    , ("src_sys_cre_by_nm", "STRING")
    , ("src_sys_updt_ts", "TIMESTAMP")
    , ("src_sys_updt_by_nm", "STRING")
    , ("src_sys_id", "STRING")
    , ("src_sys_impl_id", "STRING")
    , ("src_sys_oprtnl_stat_cd", "STRING")
    , ("src_sys_oprtnl_stat_ts", "STRING")
    , ("src_sys_btch_id","STRING")
    , ("oprtnl_stat_cd", "TINYINT")
    , ("btch_id", "STRING")
  )

  override lazy val partitionColumns = Array(
    ("proc_eff_dt", "INTEGER")
  )

  override lazy val schema = (new StructType)
    .add("file_cre_dt", StringType)
    .add("file_id", StringType)
    .add("dspnsing_oac_id", StringType)
    .add("dspnsing_oac_nm", StringType)
    .add("dspnsing_oac_addr_txt", StringType)
    .add("dspnsing_oac_pstl_cd_cd", StringType)
    .add("cff_data_strc_def_nm",StringType)
    .add("supld_data_strc_def_nm", StringType)
    .add("bus_eff_dt", TimestampType)
    .add("publ_eff_ts", TimestampType)
    .add("proc_eff_ts", TimestampType)
    .add("src_sys_cre_ts", TimestampType)
    .add("src_sys_cre_by_nm", StringType)
    .add("src_sys_updt_ts", TimestampType)
    .add("src_sys_updt_by_nm", StringType)
    .add("src_sys_id", StringType)
    .add("src_sys_impl_id", StringType)
    .add("src_sys_oprtnl_stat_cd", StringType)
    .add("src_sys_oprtnl_stat_ts", TimestampType)
    .add("src_sys_btch_id",StringType)
    .add("oprtnl_stat_cd", ShortType)
    .add("btch_id", StringType)
    .add("proc_eff_dt",IntegerType)



  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

  def helperCreatePartitionSpec(procEffDt: Int): String = s"proc_eff_dt=CAST('${procEffDt}' AS INT)"

}
