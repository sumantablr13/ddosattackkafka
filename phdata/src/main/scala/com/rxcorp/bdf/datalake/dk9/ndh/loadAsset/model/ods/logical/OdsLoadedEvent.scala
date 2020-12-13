package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical

import java.sql.Timestamp

case class OdsLoadedEvent(
                           src_impl_pit_id: Short   // DAQE
                           , src_proc_pit_id: String  //ODS
                           , data_typ_shrt_nm: String
                           , impl_btch_pit_id: String
                           , supld_dsupp_proc_id: String
                           , dervd_dsupp_file_freq_typ_cd: String
                           , dervd_dsupp_file_tm_pd_nbr: Integer
                           , ops_tbl_isrted_ts: Timestamp
                           , ops_topic_isrted_ts: Timestamp
                         )