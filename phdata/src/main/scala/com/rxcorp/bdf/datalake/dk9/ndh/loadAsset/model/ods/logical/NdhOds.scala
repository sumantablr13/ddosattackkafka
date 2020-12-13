package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical

import java.sql.Timestamp


case class NdhOds(dervd_pcmdty_lvl_cd : String,
                  supld_pcmdty_id : String,
                  supld_atc_cd : String,
                  supld_ctry_nm : String,
                  supld_region_bndry_cd : String,
                  supld_super_brick_bndry_cd : String,
                  supld_brick_bndry_cd : String,
                  supld_tot_reimbmnt_amt : String,
                  supld_tot_sale_qty : String,
                  supld_tot_pur_amt : String,
                  supld_tot_sale_amt : String,
                  supld_tot_indexed_pur_amt : String,
                  supld_tot_indexed_sale_amt : String,
                  supld_tot_sale_dddose_qty : String,
                  supld_tot_sale_cm_qty : String,
                  supld_tot_sale_iodine_qty : String,
                  supld_tot_sale_gram_qty : String,
                  supld_tot_sale_litre_qty : String,
                  supld_tot_sale_ml_qty : String,
                  supld_tot_sale_pack_qty : String,
                  supld_tot_sale_cu_qty : String,
                  supld_uniq_rx_pat_cnt : String,
                  supld_nrx_pat_cnt : String,
                  nrmlz_tot_reimbmnt_amt : String,
                  nrmlz_tot_sale_qty : String,
                  nrmlz_tot_pur_amt : String,
                  nrmlz_tot_sale_amt : String,
                  nrmlz_tot_indexed_pur_amt : String,
                  nrmlz_tot_indexed_sale_amt : String,
                  nrmlz_tot_sale_dddose_qty : String,
                  nrmlz_tot_sale_cm_qty : String,
                  nrmlz_tot_sale_iodine_qty : String,
                  nrmlz_tot_sale_gram_qty : String,
                  nrmlz_tot_sale_litre_qty : String,
                  nrmlz_tot_sale_ml_qty : String,
                  nrmlz_tot_sale_pack_qty : String,
                  nrmlz_tot_sale_cu_qty : String,
                  nrmlz_uniq_rx_pat_cnt : Integer,
                  nrmlz_nrx_pat_cnt : Integer,
                  bus_eff_dt : Timestamp,
                  bus_expry_dt : Timestamp,
                  publ_eff_ts : Timestamp,
                  proc_eff_ts : Timestamp,
                  proc_expry_ts : Timestamp,
                  oprtnl_stat_cd : String,
                  btch_id : String,
                  supld_file_id : String,
                  supld_file_cre_dt : String,
                  dervd_dsupp_proc_id : String,
                  data_proc_nm : String,
                  src_sys_cre_ts : String,
                  src_sys_cre_by_nm : String,
                  src_sys_updt_ts : String,
                  src_sys_updt_by_nm : String,
                  src_sys_id : String,
                  src_sys_oprtnl_stat_cd : String,
                  src_sys_oprtnl_stat_ts : String,
                  src_sys_btch_id : String,
                  trans_orig_typ_cd : String,
                  dervd_dsupp_cd : String,
                  dervd_pd_lvl_cd : Integer,
                  dervd_supld_pd_id : Integer) {

  def this() = this(
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
  )
}