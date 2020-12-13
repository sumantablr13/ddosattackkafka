package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

/**
 * @author rdas2 on 8/26/2020
 *
 **/
abstract class AbstractLmsDrugNames(appContext: AppContext) extends PartitionTable("") {
  override lazy val logicalName: String = "LMS DrugNames Reference"
  override lazy val physicalName: String = "dk9_lms21_pcmdty_extn"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("drug_id", "STRING"),
    ("lng_nm", "STRING"),
    ("bus_eff_dt", "TIMESTAMP"),
    ("bus_expry_dt", "TIMESTAMP"),
    ("publ_eff_ts", "TIMESTAMP"),
    ("proc_eff_ts", "TIMESTAMP"),
    ("proc_expry_ts", "TIMESTAMP"),
    ("oprtnl_stat_cd", "STRING"),
    ("btch_id", "STRING"),
    ("file_id", "STRING")
  )

  override lazy val partitionColumns: Array[(String, String)] = Array(
    ("opr_supld_pd_id", "INT")
  )

  override lazy val schema: StructType = (new StructType)
    .add("drug_id", StringType)
    .add("lng_nm", StringType)
    .add("bus_eff_dt", TimestampType)
    .add("bus_expry_dt", TimestampType)
    .add("publ_eff_ts", TimestampType)
    .add("proc_eff_ts", TimestampType)
    .add("proc_expry_ts", TimestampType)
    .add("oprtnl_stat_cd", StringType)
    .add("btch_id", StringType)
    .add("file_id", StringType)
    .add("opr_supld_pd_id", IntegerType)


  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

  def helperCreatePartitionSpec(opr_supld_pd_id: Int ): String = s"opr_supld_pd_id=CAST('${opr_supld_pd_id}' AS INT)"

}
