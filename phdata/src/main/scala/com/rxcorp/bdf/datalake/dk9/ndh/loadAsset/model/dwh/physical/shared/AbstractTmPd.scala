package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractTmPd(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Time Period"
  override lazy val physicalName: String = "tm_pd"

  override lazy val tableColumns = Array(
    ("cal_typ_nm", "STRING"),
    ("tm_pd_typ_nm", "STRING"),
    ("tm_pd_strt_dt", "TIMESTAMP"),
    ("tm_pd_end_dt", "TIMESTAMP"),
    ("tm_pd_nbr", "INT"),
    ("tm_pd_split_ind", "TINYINT"),
    ("tm_pd_shrt_nm", "STRING"),
    ("tm_pd_nm", "STRING"),
    ("assct_wk_tm_pd_nbr", "INT"),
    ("assct_wk_tm_pd_seq_id", "INT"),
    ("assct_wk_tm_pd_seq_nm", "STRING"),
    ("assct_mth_tm_pd_nbr", "INT"),
    ("assct_mth_tm_pd_seq_id", "INT"),
    ("assct_mth_tm_pd_seq_nm", "STRING"),
    ("assct_qtr_tm_pd_nbr", "INT"),
    ("assct_qtr_tm_pd_seq_id", "INT"),
    ("assct_qtr_tm_pd_seq_nm", "STRING"),
    ("assct_yr_tm_pd_nbr", "INT"),
    ("assct_yr_tm_pd_seq_id", "INT"),
    ("assct_yr_tm_pd_seq_nm", "STRING"),
    ("cre_ts", "TIMESTAMP"),
    ("cre_by_nm", "STRING"),
    ("bus_eff_dt", "TIMESTAMP"),
    ("bus_expry_dt", "TIMESTAMP"),
    ("row_not_recv_ind", "STRING"),
    ("oprtnl_stat_cd", "STRING"),
    ("proc_eff_ts", "TIMESTAMP"),
    ("proc_expry_ts", "TIMESTAMP"),
    ("proc_by_nm", "STRING"),
    ("publ_eff_ts", "TIMESTAMP"),
    ("src_sys_id", "STRING"),
    ("src_sys_impl_id", "STRING"),
    ("src_sys_cre_ts", "TIMESTAMP"),
    ("src_sys_cre_by_nm", "STRING"),
    ("src_sys_updt_ts", "TIMESTAMP"),
    ("src_sys_updt_by_nm", "STRING"),
    ("src_sys_oprtnl_stat_cd", "STRING")
  )

  override lazy val schema = StructType(Array(
    StructField("cal_typ_nm", StringType, true),
    StructField("tm_pd_typ_nm", StringType, true),
    StructField("tm_pd_strt_dt", TimestampType, true),
    StructField("tm_pd_end_dt", TimestampType ,true),
    StructField("tm_pd_nbr", IntegerType, true),
    StructField("tm_pd_split_ind", ByteType, true),
    StructField("tm_pd_shrt_nm", StringType, true),
    StructField("tm_pd_nm", StringType, true),
    StructField("assct_wk_tm_pd_nbr", IntegerType, true),
    StructField("assct_wk_tm_pd_seq_id", IntegerType, true),
    StructField("assct_wk_tm_pd_seq_nm", StringType, true),
    StructField("assct_mth_tm_pd_nbr", IntegerType, true),
    StructField("assct_mth_tm_pd_seq_id", IntegerType, true),
    StructField("assct_mth_tm_pd_seq_nm", StringType, true),
    StructField("assct_qtr_tm_pd_nbr", IntegerType, true),
    StructField("assct_qtr_tm_pd_seq_id", IntegerType, true),
    StructField("assct_qtr_tm_pd_seq_nm", StringType, true),
    StructField("assct_yr_tm_pd_nbr", IntegerType, true),
    StructField("assct_yr_tm_pd_seq_id", IntegerType, true),
    StructField("assct_yr_tm_pd_seq_nm", StringType, true),
    StructField("cre_ts", TimestampType, true),
    StructField("cre_by_nm", StringType, true),
    StructField("bus_eff_dt", TimestampType, true),
    StructField("bus_expry_dt", TimestampType, true),
    StructField("row_not_recv_ind", StringType, true),
    StructField("oprtnl_stat_cd", StringType, true),
    StructField("proc_eff_ts", TimestampType, true),
    StructField("proc_expry_ts", TimestampType, true),
    StructField("proc_by_nm", StringType, true),
    StructField("publ_eff_ts", TimestampType, true),
    StructField("src_sys_id", StringType, true),
    StructField("src_sys_impl_id", StringType, true),
    StructField("src_sys_cre_ts", TimestampType, true),
    StructField("src_sys_cre_by_nm", StringType, true),
    StructField("src_sys_updt_ts", TimestampType, true),
    StructField("src_sys_updt_by_nm", StringType ,true),
    StructField("src_sys_oprtnl_stat_cd", StringType, true)
  ))

  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """
}

