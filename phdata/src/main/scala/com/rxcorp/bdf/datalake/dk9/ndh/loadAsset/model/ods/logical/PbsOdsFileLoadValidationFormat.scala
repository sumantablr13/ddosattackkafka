package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical

import java.sql.Timestamp

case class PbsOdsFileLoadValidationFormat(
                                           ctry_iso_cd:String
                                           , data_asset_nm:String
                                           , data_cntnt_catg_cd:String
                                           , src_sys_impl_id:String
                                           , src_sys_impl_btch_id:String
                                           , dsupp_file_nm:String
                                           , dsupp_file_tm_pd_nbr:Integer
                                           , dsupp_file_tm_pd_typ_nm:String
                                           , dsupp_file_load_stat_cd:String
                                           , dsupp_file_dupe_ind:String
                                           , dsupp_file_md5sum_txt:String
                                           , dsupp_file_rec_cnt:Integer
                                           , meas1_attr_nm:String
                                           , meas1_attr_val:BigDecimal
                                           , meas2_attr_nm:String
                                           , meas2_attr_val:BigDecimal
                                           , appl_id:String
                                           , proc_eff_ts:Timestamp
                                           , proc_expry_ts:Timestamp
                                           , proc_by_nm:String
                                           , dervd_dsupp_proc_id:String
                                          )
