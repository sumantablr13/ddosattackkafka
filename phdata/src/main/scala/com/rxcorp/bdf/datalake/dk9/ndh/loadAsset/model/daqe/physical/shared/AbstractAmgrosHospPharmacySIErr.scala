package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
  * @author rdas2 on 10/4/2020
  *
  * */
abstract class AbstractAmgrosHospPharmacySIErr(appContext: AppContext) extends PartitionTable("") {
  override lazy val logicalName: String = "Sell In Amgros Hosp Pharmacy Error"
  override lazy val physicalName: String = "dk9_amgros_hosp_pharmy_si_load_err"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("item_nbr", "STRING"),
    ("sale_qty", "STRING"),
    ("turnvr_list_prc_amt", "STRING"),
    ("hosp_pharmy_cd", "STRING"),
    ("sale_dt", "STRING"),
    ("sale_pd_lvl_cd", "STRING"),
    ("ddqid", "LONG"),
    ("ddqerrorcol", "STRING"),
    ("ddqerrorconstraint", "STRING"),
    ("ddqerrorcode", "INT"),
    ("ddqts", "LONG")
  )

  override lazy val partitionColumns: Array[(String, String)] = Array(
    ("runid", "STRING")
  )


  override lazy val schema: StructType = (new StructType)
    .add("item_nbr", StringType)
    .add("sale_qty", StringType)
    .add("turnvr_list_prc_amt", StringType)
    .add("hosp_pharmy_cd", StringType)
    .add("sale_dt", StringType)
    .add("sale_pd_lvl_cd", StringType)
    .add("ddqid", LongType)
    .add("ddqerrorcol", StringType)
    .add("ddqerrorconstraint", StringType)
    .add("ddqerrorcode", IntegerType)
    .add("ddqts", LongType)
    .add("runid", StringType)

  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

  def helperCreatePartitionSpec(runid: String): String = s"runid='${runid}')"

}
