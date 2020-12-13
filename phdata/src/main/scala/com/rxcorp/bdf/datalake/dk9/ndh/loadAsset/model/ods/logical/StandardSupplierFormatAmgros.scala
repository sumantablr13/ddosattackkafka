package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical

/**
  * @author rdas2 on 10/4/2020
  *
  * */
case class StandardSupplierFormatAmgros(item_nbr: String,
                                        sale_qty: String,
                                        turnvr_list_prc_amt: String,
                                        hosp_pharmy_cd: String,
                                        sale_dt: String,
                                        sale_pd_lvl_cd: String,
                                        ddqid: Long,
                                        ddqts: Long,
                                        runid: String) extends StandardSupplier

object StandardSupplierFormatAmgros{
  final val CopyString = s""
}
