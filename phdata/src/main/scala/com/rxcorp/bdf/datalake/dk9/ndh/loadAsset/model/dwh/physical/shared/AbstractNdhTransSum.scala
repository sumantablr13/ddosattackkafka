package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types._

/**
  * @author rdas2 on 9/22/2020
  *
  * */
abstract class AbstractNdhTransSum(appContext: AppContext) extends PartitionTable("") {
  override lazy val logicalName: String = "SellOut Transaction Sum"
  override lazy val physicalName: String = "dk9_so_trans_sum"

  override lazy val tableColumns = Array(
    ("dervd_pcmdty_lvl_cd", "STRING"),
    ("supld_pcmdty_id", "STRING"),
    ("supld_atc_cd", "STRING"),
    ("supld_ctry_nm", "STRING"),
    ("supld_region_bndry_cd", "STRING"),
    ("supld_super_brick_bndry_cd", "STRING"),
    ("supld_brick_bndry_cd", "STRING"),
    ("supld_tot_reimbmnt_amt", "STRING"),
    ("supld_tot_sale_qty", "STRING"),
    ("supld_tot_pur_amt", "STRING"),
    ("supld_tot_sale_amt", "STRING"),
    ("supld_tot_indexed_pur_amt", "STRING"),
    ("supld_tot_indexed_sale_amt", "STRING"),
    ("supld_tot_sale_dddose_qty", "STRING"),
    ("supld_tot_sale_cm_qty", "STRING"),
    ("supld_tot_sale_iodine_qty", "STRING"),
    ("supld_tot_sale_gram_qty", "STRING"),
    ("supld_tot_sale_litre_qty", "STRING"),
    ("supld_tot_sale_ml_qty", "STRING"),
    ("supld_tot_sale_pack_qty", "STRING"),
    ("supld_tot_sale_cu_qty", "STRING"),
    ("supld_uniq_rx_pat_cnt", "STRING"),
    ("supld_nrx_pat_cnt", "STRING"),
    ("nrmlz_tot_reimbmnt_amt", "DECIMAL"),
    ("nrmlz_tot_sale_qty", "DECIMAL"),
    ("nrmlz_tot_pur_amt", "DECIMAL"),
    ("nrmlz_tot_sale_amt", "DECIMAL"),
    ("nrmlz_tot_indexed_pur_amt", "DECIMAL"),
    ("nrmlz_tot_indexed_sale_amt", "DECIMAL"),
    ("nrmlz_tot_sale_dddose_qty", "DECIMAL"),
    ("nrmlz_tot_sale_cm_qty", "DECIMAL"),
    ("nrmlz_tot_sale_iodine_qty", "DECIMAL"),
    ("nrmlz_tot_sale_gram_qty", "DECIMAL"),
    ("nrmlz_tot_sale_litre_qty", "DECIMAL"),
    ("nrmlz_tot_sale_ml_qty", "DECIMAL"),
    ("nrmlz_tot_sale_pack_qty", "DECIMAL"),
    ("nrmlz_tot_sale_cu_qty", "DECIMAL"),
    ("nrmlz_uniq_rx_pat_cnt", "INTEGER"),
    ("nrmlz_nrx_pat_cnt", "INTEGER"),
    ("bus_eff_dt", "TIMESTAMP"),
    ("bus_expry_dt", "TIMESTAMP"),
    ("publ_eff_ts", "TIMESTAMP"),
    ("proc_eff_ts", "TIMESTAMP"),
    ("proc_expry_ts", "TIMESTAMP"),
    ("oprtnl_stat_cd", "STRING"),
    ("btch_id", "STRING"),
    ("supld_file_id", "STRING"),
    ("supld_file_cre_dt", "STRING"),
    ("dervd_dsupp_proc_id", "STRING"),
    ("data_proc_nm", "STRING"),
    ("src_sys_cre_ts", "STRING"),
    ("src_sys_cre_by_nm", "STRING"),
    ("src_sys_updt_ts", "STRING"),
    ("src_sys_updt_by_nm", "STRING"),
    ("src_sys_id", "STRING"),
    ("src_sys_oprtnl_stat_cd", "STRING"),
    ("src_sys_oprtnl_stat_ts", "STRING"),
    ("src_sys_btch_id", "STRING"),
    ("trans_orig_typ_cd", "STRING")
  )

  override lazy val partitionColumns = Array(
    ("dervd_dsupp_cd", "STRING"),
    ("dervd_pd_lvl_cd", "INTEGER"),
    ("dervd_supld_pd_id", "INTEGER")
  )

  override lazy val schema = (new StructType)
    .add("dervd_pcmdty_lvl_cd",StringType)
    .add("supld_pcmdty_id",StringType)
    .add("supld_atc_cd",StringType)
    .add("supld_ctry_nm",StringType)
    .add("supld_region_bndry_cd",StringType)
    .add("supld_super_brick_bndry_cd",StringType)
    .add("supld_brick_bndry_cd",StringType)
    .add("supld_tot_reimbmnt_amt",StringType)
    .add("supld_tot_sale_qty",StringType)
    .add("supld_tot_pur_amt",StringType)
    .add("supld_tot_sale_amt",StringType)
    .add("supld_tot_indexed_pur_amt",StringType)
    .add("supld_tot_indexed_sale_amt",StringType)
    .add("supld_tot_sale_dddose_qty",StringType)
    .add("supld_tot_sale_cm_qty",StringType)
    .add("supld_tot_sale_iodine_qty",StringType)
    .add("supld_tot_sale_gram_qty",StringType)
    .add("supld_tot_sale_litre_qty",StringType)
    .add("supld_tot_sale_ml_qty",StringType)
    .add("supld_tot_sale_pack_qty",StringType)
    .add("supld_tot_sale_cu_qty",StringType)
    .add("supld_uniq_rx_pat_cnt",StringType)
    .add("supld_nrx_pat_cnt",StringType)
    .add("nrmlz_tot_reimbmnt_amt", DecimalType(38,4))
    .add("nrmlz_tot_sale_qty", DecimalType(38,4))
    .add("nrmlz_tot_pur_amt",DecimalType(38,4))
    .add("nrmlz_tot_sale_amt",DecimalType(38,4))
    .add("nrmlz_tot_indexed_pur_amt",DecimalType(38,4))
    .add("nrmlz_tot_indexed_sale_amt",DecimalType(38,4))
    .add("nrmlz_tot_sale_dddose_qty",DecimalType(38,4))
    .add("nrmlz_tot_sale_cm_qty",DecimalType(38,4))
    .add("nrmlz_tot_sale_iodine_qty",DecimalType(38,4))
    .add("nrmlz_tot_sale_gram_qty",DecimalType(38,4))
    .add("nrmlz_tot_sale_litre_qty",DecimalType(38,4))
    .add("nrmlz_tot_sale_ml_qty",DecimalType(38,4))
    .add("nrmlz_tot_sale_pack_qty",DecimalType(38,4))
    .add("nrmlz_tot_sale_cu_qty",DecimalType(38,4))
    .add("nrmlz_uniq_rx_pat_cnt",IntegerType)
    .add("nrmlz_nrx_pat_cnt",IntegerType)
    .add("bus_eff_dt",TimestampType)
    .add("bus_expry_dt",TimestampType)
    .add("publ_eff_ts",TimestampType)
    .add("proc_eff_ts",TimestampType)
    .add("proc_expry_ts",TimestampType)
    .add("oprtnl_stat_cd",StringType)
    .add("btch_id",StringType)
    .add("supld_file_id",StringType)
    .add("supld_file_cre_dt",StringType)
    .add("dervd_dsupp_proc_id",StringType)
    .add("data_proc_nm",StringType)
    .add("src_sys_cre_ts",StringType)
    .add("src_sys_cre_by_nm",StringType)
    .add("src_sys_updt_ts",StringType)
    .add("src_sys_updt_by_nm",StringType)
    .add("src_sys_id",StringType)
    .add("src_sys_oprtnl_stat_cd",StringType)
    .add("src_sys_oprtnl_stat_ts",StringType)
    .add("src_sys_btch_id",StringType)
    .add("trans_orig_typ_cd",StringType)
    .add("dervd_dsupp_cd",StringType)
    .add("dervd_pd_lvl_cd",IntegerType)
    .add("dervd_supld_pd_id",IntegerType)


  override lazy val createTableOpts =
    s"""
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
  """

  def helperCreatePartitionSpec(dervd_dsupp_cd: String,dervd_pd_lvl_cd: Int, dervd_supld_pd_id:Int): String =
    s"(dervd_dsupp_cd='${dervd_dsupp_cd}', dervd_pd_lvl_cd = CAST('$dervd_pd_lvl_cd' AS INT), dervd_supld_pd_id=CAST('${dervd_supld_pd_id}' AS INT))"

}
