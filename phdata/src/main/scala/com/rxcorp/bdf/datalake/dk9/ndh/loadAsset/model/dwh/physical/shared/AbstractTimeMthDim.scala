package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractTimeMthDim(appContext: AppContext) extends Table("") {

  override lazy val logicalName: String = "Time Month Dimension"
  override lazy val physicalName: String = "v_tm_pd_gre_mth_dim"

  override lazy val tableColumns = Array(
    ("tm_pd_id", "int"),
    ("mth_strt_dt", "string"),
    ("mth_end_dt", "string"),
    ("mth_shrt_nm", "string"),
    ("mth_nm", "string"),
    ("mth_nbr", "int"),
    ("qtr_tm_pd_id", "int"),
    ("qtr_strt_dt", "string"),
    ("qtr_end_dt", "string"),
    ("qtr_shrt_nm", "string"),
    ("qtr_nm", "string"),
    ("qtr_nbr", "int"),
    ("mth_in_qtr_nbr", "int"),
    ("half_yr_tm_pd_id", "int"),
    ("half_yr_strt_dt", "string"),
    ("half_yr_end_dt", "string"),
    ("half_yr_shrt_nm", "string"),
    ("half_yr_nm", "string"),
    ("half_yr_nbr", "int"),
    ("mth_in_half_yr_nbr", "int"),
    ("yr_tm_pd_id", "int"),
    ("yr_strt_dt", "string"),
    ("yr_end_dt", "string"),
    ("yr_shrt_nm", "string"),
    ("yr_nm", "string"),
    ("yr_nbr", "int"),
    ("mth_in_yr_nbr", "int"),
    ("publ_eff_ts", "string"),
    ("bus_eff_dt", "string"),
    ("publ_expry_ts", "string"),
    ("bus_expry_dt", "string")
  )

  override lazy val schema = StructType(Array(
      StructField("tm_pd_id", IntegerType,true),
      StructField("mth_strt_dt", StringType,true),
      StructField("mth_end_dt", StringType,true),
      StructField("mth_shrt_nm", StringType,true),
      StructField("mth_nm", StringType,true),
      StructField("mth_nbr", IntegerType,true),
      StructField("qtr_tm_pd_id", IntegerType,true),
      StructField("qtr_strt_dt", StringType,true),
      StructField("qtr_end_dt", StringType,true),
      StructField("qtr_shrt_nm", StringType,true),
      StructField("qtr_nm", StringType,true),
      StructField("qtr_nbr", IntegerType,true),
      StructField("mth_in_qtr_nbr", IntegerType,true),
      StructField("half_yr_tm_pd_id", IntegerType,true),
      StructField("half_yr_strt_dt", StringType,true),
      StructField("half_yr_end_dt", StringType,true),
      StructField("half_yr_shrt_nm", StringType,true),
      StructField("half_yr_nm", StringType,true),
      StructField("half_yr_nbr", IntegerType,true),
      StructField("mth_in_half_yr_nbr", IntegerType,true),
      StructField("yr_tm_pd_id", IntegerType,true),
      StructField("yr_strt_dt", StringType,true),
      StructField("yr_end_dt", StringType,true),
      StructField("yr_shrt_nm", StringType,true),
      StructField("yr_nm", StringType,true),
      StructField("yr_nbr", IntegerType,true),
      StructField("mth_in_yr_nbr", IntegerType,true),
      StructField("publ_eff_ts", StringType,true),
      StructField("bus_eff_dt", StringType,true),
      StructField("publ_expry_ts", StringType,true),
      StructField("bus_expry_dt", StringType,true)
  ))

  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """
}
