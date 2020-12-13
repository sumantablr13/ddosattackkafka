package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical

/**
  * Created by DJha on 25/03/2018.
  */
case class StandardSupplierFormat(
                                     file_cre_dt : String
                                   , file_id : String
                                   , dspnsing_oac_id : String
                                   , dspnsd_rx_trans_id : String
                                   , dspnsd_rx_grp_id : String
                                   , dspnsd_rx_item_seq_nbr : String
                                   , rx_dspnsd_dt : String
                                   , rx_dspnsd_tm : String
                                   , pscrng_medpro_encrypt_id : String
                                   , dspnsing_sys_rec_typ_cd : String
                                   , dspnsd_rx_collected_ind : String
                                   , dspnsd_rx_pat_instr_txt : String
                                   , part_fill_rmng_unit_qty : String
                                   , nrsg_hm_ind : String
                                   , med_diag_cd_list_txt : String
                                   , reimbmnt_catg_cd : String
                                   , rx_typ_cd : String
                                   , rx_fill_typ_cd : String
                                   , rx_fill_st_cd : String
                                   , cdd_genl_practnr_qty_txt : String
                                   , dspnsd_qty : String
                                   , pscr_qty : String
                                   , dspnsd_rx_item_uom_cd : String
                                   , dspnsd_rx_item_uom_txt : String
                                   , dspnsd_multilex_pip_id : String
                                   , pscr_multilex_pip_id : String
                                   , dspnsd_mediphase_pip_id : String
                                   , pscr_mediphase_pip_id : String
                                   , dspnsd_pip_id : String
                                   , pscr_pip_id : String
                                   , dspnsd_prosper_id : String
                                   , pscr_prosper_id : String
                                   , dspnsd_link_id : String
                                   , pscr_link_id : String
                                   , dspnsd_dmd_id : String
                                   , pscr_dmd_id : String
                                   , dspnsd_nexphase_id : String
                                   , pscr_nexphase_id : String
                                   , dspnsd_mps_id : String
                                   , pscr_mps_id : String
                                   , dspnsd_proscript_id : String
                                   , pscr_proscript_id : String
                                   , dspnsd_pmr1_id : String
                                   , pscr_pmr1_id : String
                                   , dspnsd_pmr2_id : String
                                   , pscr_pmr2_id : String
                                   , dspnsd_othr1_extrnl_id : String
                                   , pscr_othr1_extrnl_id : String
                                   , dspnsd_othr2_extrnl_id : String
                                   , pscr_othr2_extrnl_id : String
                                   , dspnsd_pcmdty_desc : String
                                   , pscr_pcmdty_desc : String
                                   , dspnsd_cmdty_prof_qty : String
                                   , pscr_cmdty_prof_qty : String
                                   , dspnsd_cmdty_strnt_txt : String
                                   , pscr_cmdty_strnt_txt : String
                                   , dspnsd_generic_pcmdty_ind : String
                                   , pscr_generic_pcmdty_ind : String
                                   , dspnsd_ean13_id : String
                                   , sllr_expc_pay_amt : String
                                   , bus_eff_dt : String
                                   , publ_eff_ts: String
                                   , proc_eff_ts: String
                                   , proc_eff_dt: Integer
                                   , src_sys_cre_ts: String
                                   , src_sys_cre_by_nm: String
                                   , src_sys_updt_ts: String
                                   , src_sys_updt_by_nm: String
                                   , src_sys_id: String
                                   , src_sys_impl_id: String
                                   , src_sys_oprtnl_stat_cd: String
                                   , src_sys_oprtnl_stat_ts: String
                                   , src_sys_btch_id: String
                                   , oprtnl_stat_cd: Integer
                                   , btch_id: String
                                   , dervd_dsupp_trans_id: String
                                   , dspnsing_oac_nm: String
                                   , dspnsing_oac_addr_txt: String
                                   , dspnsing_oac_pstl_cd_cd: String
                                   , supld_data_strc_def_nm: String
                                   , cff_data_strc_def_nm: String
                                 )extends StandardSupplierToString