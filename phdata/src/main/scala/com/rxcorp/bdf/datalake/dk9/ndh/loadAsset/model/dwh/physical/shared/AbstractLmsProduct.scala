package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types._

abstract class AbstractLmsProduct(appContext: AppContext) extends PartitionTable(""){
  override lazy val logicalName: String = "LMS Product Reference"
  override lazy val physicalName: String = "dk9_lms01_pcmdty"

  override lazy val tableColumns = Array(
    ("drug_id","STRING"),
    ("prod_catg","STRING"),
    ("prod_subcatg","STRING"),
    ("alphbt_seq_space","STRING"),
    ("spec_nbr","STRING"),
    ("nm","STRING"),
    ("form","STRING"),
    ("form_cd","STRING"),
    ("addl_form_info_cd","STRING"),
    ("strnt_txt","STRING"),
    ("strnt_nbr","STRING"),
    ("unit","STRING"),
    ("mkter","STRING"),
    ("distr","STRING"),
    ("atc","STRING"),
    ("adm_route","STRING"),
    ("trffc_warng","STRING"),
    ("substtn","STRING"),
    ("blank1","STRING"),
    ("blank2","STRING"),
    ("blank3","STRING"),
    ("substtn_grp","STRING"),
    ("suitbl_dspnsing_dose","STRING"),
    ("deactvtn_dt","STRING"),
    ("quarantine_dt","STRING"),
    ("bus_eff_dt", "TIMESTAMP"),
    ("bus_expry_dt", "TIMESTAMP"),
    ("publ_eff_ts", "TIMESTAMP"),
    ("proc_eff_ts", "TIMESTAMP"),
    ("proc_expry_ts", "TIMESTAMP"),
    ("oprtnl_stat_cd", "STRING"),
    ("btch_id", "STRING"),
    ("file_id", "STRING")
  )

  override lazy val partitionColumns = Array(
	  ("opr_supld_pd_id", "INT")
  )

  override lazy val schema = (new StructType)
    .add("drug_id", StringType)
    .add("prod_catg", StringType)
    .add("prod_subcatg", StringType)
    .add("alphbt_seq_space", StringType)
    .add("spec_nbr", StringType)
    .add("nm", StringType)
    .add("form", StringType)
    .add("form_cd", StringType)
    .add("addl_form_info_cd", StringType)
    .add("strnt_txt", StringType)
    .add("strnt_nbr", StringType)
    .add("unit", StringType)
    .add("mkter", StringType)
    .add("distr", StringType)
    .add("atc", StringType)
    .add("adm_route", StringType)
    .add("trffc_warng", StringType)
    .add("substtn", StringType)
    .add("blank1", StringType)
    .add("blank2", StringType)
    .add("blank3", StringType)
    .add("substtn_grp", StringType)
    .add("suitbl_dspnsing_dose", StringType)
    .add("deactvtn_dt", StringType)
    .add("quarantine_dt", StringType)
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
