package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

/**
 * @author rdas2 on 8/24/2020
 *
 **/
abstract class AbstractLmsPack(appContext: AppContext) extends PartitionTable("") {
  override lazy val logicalName: String = "LMS Pack Reference"
  override lazy val physicalName: String = "dk9_lms02_pcmdty_prof_packng"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("drug_id", "STRING"),
    ("item_cd", "STRING"),
    ("alphbt_seq", "STRING"),
    ("subpack_item_cd", "STRING"),
    ("subpack_cnt", "STRING"),
    ("pack_size_txt", "STRING"),
    ("pack_size_nbr", "STRING"),
    ("pack_size_unit", "STRING"),
    ("packng_typ", "STRING"),
    ("handout_prvsn", "STRING"),
    ("handout_spcl", "STRING"),
    ("reimbmnt_cd", "STRING"),
    ("reimbmnt_clause", "STRING"),
    ("ddd_packing_cnt", "STRING"),
    ("sto_tm", "STRING"),
    ("sto_tm_unit", "STRING"),
    ("sto_cond", "STRING"),
    ("creatn_dt", "STRING"),
    ("latest_prc_chg_dt", "STRING"),
    ("exprd_dt", "STRING"),
    ("calc_cd_aip", "STRING"),
    ("pakage_reimbmnt_grp", "STRING"),
    ("prep_fee", "STRING"),
    ("safety_feat", "STRING"),
    ("packag_distr", "STRING"),
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
    .add("item_cd", StringType)
    .add("alphbt_seq", StringType)
    .add("subpack_item_cd", StringType)
    .add("subpack_cnt", StringType)
    .add("pack_size_txt", StringType)
    .add("pack_size_nbr", StringType)
    .add("pack_size_unit", StringType)
    .add("packng_typ", StringType)
    .add("handout_prvsn", StringType)
    .add("handout_spcl", StringType)
    .add("reimbmnt_cd", StringType)
    .add("reimbmnt_clause", StringType)
    .add("ddd_packing_cnt", StringType)
    .add("sto_tm", StringType)
    .add("sto_tm_unit", StringType)
    .add("sto_cond", StringType)
    .add("creatn_dt", StringType)
    .add("latest_prc_chg_dt", StringType)
    .add("exprd_dt", StringType)
    .add("calc_cd_aip", StringType)
    .add("pakage_reimbmnt_grp", StringType)
    .add("prep_fee", StringType)
    .add("safety_feat", StringType)
    .add("packag_distr", StringType)
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
