package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types._

/*
    Created by SMydur on 09/05/2018
 */

abstract class AbstractNdhFactOdsErrLog(appContext: AppContext) extends PartitionTable("") {

  override lazy val logicalName: String = "Prescription Data Sell In Transaction Acquisition Error Log"
  override lazy val physicalName: String = "dk9_so_trans_sum_acq_err_log"

  override lazy val tableColumns = Array(
    ("dervd_dsupp_proc_id", "STRING")
    , ("file_id", "STRING")
    , ("dspnsd_rx_trans_id", "STRING")
    , ("err_typ_nm", "STRING")
    , ("err_cd", "STRING")
    , ("err_desc", "STRING")
    , ("err_clmn", "STRING")
    , ("err_attr_val", "STRING")
    , ("src_sys_impl_id", "STRING")
    , ("btch_id", "STRING")
    , ("proc_nm", "STRING")
    , ("cre_by_nm", "STRING")
    , ("bus_eff_dt", "TIMESTAMP")
    , ("oprtnl_stat_cd", "STRING")
    , ("proc_eff_ts", "TIMESTAMP")
    , ("publ_eff_ts", "TIMESTAMP")
  )

  override lazy val partitionColumns = Array(("proc_eff_dt", "INT"))

  override lazy val schema: StructType = (new StructType)
    .add("dervd_dsupp_proc_id", StringType)
    .add("file_id", StringType)
    .add("dspnsd_rx_trans_id", StringType)
    .add("err_typ_nm", StringType)
    .add("err_cd", StringType)
    .add("err_desc", StringType)
    .add("err_clmn", StringType)
    .add("err_attr_val", StringType)
    .add("src_sys_impl_id", StringType)
    .add("btch_id", StringType)
    .add("proc_nm", StringType)
    .add("cre_by_nm", StringType)
    .add("bus_eff_dt", TimestampType)
    .add("oprtnl_stat_cd", StringType)
    .add("proc_eff_ts", TimestampType)
    .add("publ_eff_ts", TimestampType)
    .add("proc_eff_dt", IntegerType)

  override lazy val createTableOpts =
    """
       |ROW FORMAT DELIMITED
       |FIELDS TERMINATED BY '\001'
       |LINES TERMINATED BY '\n'
       |STORED AS TEXTFILE
    """.stripMargin

  def helperCreatePartitionSpec(procEffDt: Int): String = s"proc_eff_dt=CAST('$procEffDt' AS INT)"
}
