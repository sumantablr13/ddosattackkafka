package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.logical

case class LmsProductFormat
(
  drug_id:String,
  prod_catg:String,
  prod_subcatg:String,
  alphbt_seq_space:String,
  spec_nbr:String,
  nm:String,
  form:String,
  form_cd:String,
  addl_form_info_cd:String,
  strnt_txt:String,
  strnt_nbr:String,
  unit:String,
  mkter:String,
  distr:String,
  atc:String,
  adm_route:String,
  trffc_warng:String,
  substtn:String,
  blank1:String,
  blank2:String,
  blank3:String,
  substtn_grp:String,
  suitbl_dspnsing_dose:String,
  deactvtn_dt:String,
  quarantine_dt:String
) {

}


object LmsProductFormat{
  final val fileTimeFormat: String = "yyyy-MM-dd HH:mm:ss"
  final val fileColumnsSize: Array[Int] = Array(11,2,2,9,5,30,20,7,7,20,10,3,6,6,8,8,1,1,1,1,1,4,1,8,8)
  final val LMS_FILE_TYPE = "PRODUCT"
}

case class LmsPackFormat(drug_id: String,
                         item_cd: String,
                         alphbt_seq: String,
                         subpack_item_cd: String,
                         subpack_cnt: String,
                         pack_size_txt: String,
                         pack_size_nbr: String,
                         pack_size_unit: String,
                         packng_typ: String,
                         handout_prvsn: String,
                         handout_spcl: String,
                         reimbmnt_cd: String,
                         reimbmnt_clause: String,
                         ddd_packing_cnt: String,
                         sto_tm: String,
                         sto_tm_unit: String,
                         sto_cond: String,
                         creatn_dt: String,
                         latest_prc_chg_dt: String,
                         exprd_dt: String,
                         calc_cd_aip: String,
                         pakage_reimbmnt_grp: String,
                         prep_fee: String,
                         safety_feat: String,
                         packag_distr: String)

object LmsPackFormat {
  final val fileTimeFormat: String = "yyyy-MM-dd HH:mm:ss"
  final val fileColumnsSize: Array[Int] = Array(11,6,3,6,3,30,8,2,4,5,5,2,5,9,2,1,1,8,8,8,1,1,1,1,6)
  final val LMS_FILE_TYPE = "PACK"
}

case class LmsCompanyFormat(
                             org_id: String,
                             org_shrt_nm: String,
                             org_lng_nm: String,
                             pi_cd: String
                           )

object LmsCompanyFormat {
  final val fileTimeFormat: String = "yyyy-MM-dd HH:mm:ss"
  final val fileColumnsSize: Array[Int] = Array(6,20,32,2)
  final val LMS_FILE_TYPE = "COMPANY"
}

case class LmsDrugNamesFormat(drug_id: String, lng_nm: String)

object LmsDrugNamesFormat{
  final val fileTimeFormat: String = "yyyy-MM-dd HH:mm:ss"
  final val fileColumnsSize: Array[Int] = Array(11,60)
  final val LMS_FILE_TYPE = "DRUG_NAMES"
}


