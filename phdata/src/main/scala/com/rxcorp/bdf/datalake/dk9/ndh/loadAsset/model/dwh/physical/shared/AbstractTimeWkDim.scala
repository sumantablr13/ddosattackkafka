package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractTimeWkDim(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Time Week Dimension"
  override lazy val physicalName: String = "v_tm_pd_gre_wk_dim"

  override lazy val tableColumns = Array(
    ("tm_pd_id", "int"),
    ("wk_strt_day_nm", "string"),
    ("wk_strt_dt", "string"),
    ("wk_end_dt", "string"),
    ("wk_shrt_nm", "string"),
    ("wk_nm", "string"),
    ("wk_nbr", "int"),
    ("yr_strt_dt", "string"),
    ("yr_end_dt", "string"),
    ("yr_shrt_nm", "string"),
    ("yr_nbr", "int"),
    ("dspnsd_rx_prd_wk_nbr", "int"),
    ("dspnsd_rx_prd_yr_nbr", "int"),
    ("dspsnd_rx_prd_yr_wk_nbr", "int"),
    ("bus_eff_dt", "string"),
    ("publ_eff_ts", "string"),
    ("publ_expry_ts", "string"),
    ("bus_expry_dt", "string")
  )

  override lazy val schema = StructType(Array(
    StructField("tm_pd_id", IntegerType, true),
    StructField("wk_strt_day_nm", StringType, true),
    StructField("wk_strt_dt", StringType, true),
    StructField("wk_end_dt", StringType, true),
    StructField("wk_shrt_nm", StringType, true),
    StructField("wk_nm", StringType, true),
    StructField("wk_nbr", IntegerType, true),
    StructField("yr_strt_dt", StringType, true),
    StructField("yr_end_dt", StringType, true),
    StructField("yr_shrt_nm", StringType, true),
    StructField("yr_nbr", IntegerType, true),
    StructField("dspnsd_rx_prd_wk_nbr", IntegerType, true),
    StructField("dspnsd_rx_prd_yr_nbr", IntegerType, true),
    StructField("dspsnd_rx_prd_yr_wk_nbr", IntegerType, true),
    StructField("bus_eff_dt", StringType, true),
    StructField("publ_eff_ts", StringType, true),
    StructField("publ_expry_ts", StringType, true),
    StructField("bus_expry_dt", StringType, true)
  ))

  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """
}

