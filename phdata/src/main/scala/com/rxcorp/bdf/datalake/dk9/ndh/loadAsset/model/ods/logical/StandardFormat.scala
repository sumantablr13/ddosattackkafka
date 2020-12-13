package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical

/**
  * @author rdas2 on 9/20/2020
  *
  * */
trait StandardFormat {
  val pcmdty_id: String
  val atc_cd: String
  val ctry_nm: String
  val region_bndry_cd: String
  val super_brick_bndry_cd: String
  val brick_bndry_cd: String
  val tot_reimbmnt_amt: String
  val tot_sale_qty: String
  val tot_pur_amt: String
  val tot_sale_amt: String
  val tot_indexed_pur_amt: String
  val tot_indexed_sale_amt: String
  val tot_sale_dddose_qty: String
  val tot_sale_cm_qty: String
  val tot_sale_iodine_qty: String
  val tot_sale_gram_qty: String
  val tot_sale_litre_qty: String
  val tot_sale_ml_qty: String
  val tot_sale_pack_qty: String
  val tot_sale_cu_qty: String
  val uniq_rx_pat_cnt: String
  val nrx_pat_cnt: String
}
