package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.logical

import java.sql.Timestamp

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.TimestampConverter


case class CCTmPd(cal_typ_nm: String,
                  tm_pd_typ_nm: String,
                  tm_pd_strt_dt: Timestamp,
                  tm_pd_end_dt: Timestamp,
                  tm_pd_nbr: Integer,
                  tm_pd_split_ind: Short,
                  tm_pd_shrt_nm: String,
                  tm_pd_nm: String,
                  assct_wk_tm_pd_nbr: Integer,
                  assct_wk_tm_pd_seq_id: Integer,
                  assct_wk_tm_pd_seq_nm: String,
                  assct_mth_tm_pd_nbr: Integer,
                  assct_mth_tm_pd_seq_id: Integer,
                  assct_mth_tm_pd_seq_nm: String,
                  assct_qtr_tm_pd_nbr: Integer,
                  assct_qtr_tm_pd_seq_id: Integer,
                  assct_qtr_tm_pd_seq_nm: String,
                  assct_yr_tm_pd_nbr: Integer,
                  assct_yr_tm_pd_seq_id: Integer,
                  assct_yr_tm_pd_seq_nm: String,
                  cre_ts: Timestamp,
                  cre_by_nm: String,
                  bus_eff_dt: Timestamp,
                  bus_expry_dt: Timestamp,
                  row_not_recv_ind: String,
                  oprtnl_stat_cd: String,
                  proc_eff_ts: Timestamp,
                  proc_expry_ts: Timestamp,
                  proc_by_nm: String,
                  publ_eff_ts: Timestamp,
                  src_sys_id: String,
                  src_sys_impl_id: String,
                  src_sys_cre_ts: Timestamp,
                  src_sys_cre_by_nm: String,
                  src_sys_updt_ts: Timestamp,
                  src_sys_updt_by_nm: String,
                  src_sys_oprtnl_stat_cd: String) {

  def this() = {
    this(
      null,    // cal_typ_nm : String,
      null,    // tm_pd_typ_nm : String,
      null,    // tm_pd_strt_dt: Timestamp,
      null,    // tm_pd_end_dt: Timestamp,
      null,    // tm_pd_nbr : Int,
      0,       // tm_pd_split_ind : Int,
      null,    // tm_pd_shrt_nm : String,
      null,    // tm_pd_nm : String,
      null,    // assct_wk_tm_pd_nbr : Int,
      null,    // assct_wk_tm_pd_seq_id : Int,
      null,    // assct_wk_tm_pd_seq_nm : String,
      null,    // assct_mth_tm_pd_nbr : Int,
      null,    // assct_mth_tm_pd_seq_id : Int,
      null,    // assct_mth_tm_pd_seq_nm : String,
      null,    // assct_qtr_tm_pd_nbr : Int,
      null,    // assct_qtr_tm_pd_seq_id :Int,
      null,    // assct_qtr_tm_pd_seq_nm : String,
      null,    // assct_yr_tm_pd_nbr : Int,
      null,    // assct_yr_tm_pd_seq_id : Int,
      null,    // assct_yr_tm_pd_seq_nm : String,
      new java.sql.Timestamp(new TimestampConverter().unixTime), // cre_ts : Timestamp,
      null,    // cre_by_nm : String,
      null,    // bus_eff_dt : Timestamp,
      null,    // bus_expry_dt : Timestamp,
      null,    // row_not_recv_ind : String,
      null,    // oprtnl_stat_cd : String,
      null,    // proc_eff_ts : Timestamp,
      null,    // proc_expry_ts : Timestamp,
      null,    // proc_by_nm : String,
      null,    // publ_eff_ts : Timestamp,
      null,    // src_sys_id : String,
      null,    // src_sys_impl_id : String,
      null,    // src_sys_cre_ts : Timestamp,
      null,    //  src_sys_cre_by_nm : String,
      null,    // src_sys_updt_ts : Timestamp,
      null,    // src_sys_updt_by_nm
      null     // src_sys_oprtnl_stat_cd : String
    )
  }
}
object CCTmPd{
  final val TM_PD_TYP_NM_MTH: String = "MONTHLY"
  final val TM_PD_TYP_NM_WK: String = "WEEKLY"
  final val TM_PD_TYP_NM_DAY: String = "DAILY"
}
