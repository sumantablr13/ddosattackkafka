package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types.{LongType, StringType, StructType}

/**
  * @author rdas2 on 9/25/2020
  *
  * */
abstract class AbstractDaqeSSTTbl1(appContext: AppContext) extends PartitionTable("") {
  override lazy val logicalName: String = "SellOut ATC redeemed per month"
  override lazy val physicalName: String = "dk9_sst_tbl_1_load"

  override lazy val tableColumns = Array(
    ("uniq_rx_pat_cnt", "STRING"),
    ("nrx_pat_cnt", "STRING"),
    ("atc_cd", "STRING"),
    ("yr_nbr", "STRING"),
    ("mth_nbr", "STRING"),
    ("ctry_nm", "STRING"),
    ("region_cd", "STRING"),
    ("super_brick_cd", "STRING"),
    ("brick_cd", "STRING"),
    ("reimbmnt_amt", "STRING"),
    ("tot_sale_qty", "STRING"),
    ("tot_pur_amt", "STRING"),
    ("tot_sale_amt", "STRING"),
    ("tot_indexed_pur_amt", "STRING"),
    ("tot_indexed_sale_amt", "STRING"),
    ("tot_sale_dddose_qty", "STRING"),
    ("tot_sale_cm_qty", "STRING"),
    ("tot_sale_iodine_qty", "STRING"),
    ("tot_sale_gram_qty", "STRING"),
    ("tot_sale_litre_qty", "STRING"),
    ("tot_sale_ml_qty", "STRING"),
    ("tot_sale_pack_qty", "STRING"),
    ("tot_sale_cu_qty", "STRING"),
    ("ddqid", "LONG"),
    ("ddqts", "LONG")
  )

  override lazy val partitionColumns = Array(
    ("runid", "STRING")
  )


  override lazy val schema = (new StructType)
    .add("uniq_rx_pat_cnt", StringType)
    .add("nrx_pat_cnt", StringType)
    .add("atc_cd", StringType)
    .add("yr_nbr", StringType)
    .add("mth_nbr", StringType)
    .add("ctry_nm", StringType)
    .add("region_cd", StringType)
    .add("super_brick_cd", StringType)
    .add("brick_cd", StringType)
    .add("reimbmnt_amt", StringType)
    .add("tot_sale_qty", StringType)
    .add("tot_pur_amt", StringType)
    .add("tot_sale_amt", StringType)
    .add("tot_indexed_pur_amt", StringType)
    .add("tot_indexed_sale_amt", StringType)
    .add("tot_sale_dddose_qty", StringType)
    .add("tot_sale_cm_qty", StringType)
    .add("tot_sale_iodine_qty", StringType)
    .add("tot_sale_gram_qty", StringType)
    .add("tot_sale_litre_qty", StringType)
    .add("tot_sale_ml_qty", StringType)
    .add("tot_sale_pack_qty", StringType)
    .add("tot_sale_cu_qty", StringType)
    .add("ddqid", LongType)
    .add("ddqts", LongType)
    .add("runid", StringType)

  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

  def helperCreatePartitionSpec(runid: String): String = s"runid='${runid}')"

}
