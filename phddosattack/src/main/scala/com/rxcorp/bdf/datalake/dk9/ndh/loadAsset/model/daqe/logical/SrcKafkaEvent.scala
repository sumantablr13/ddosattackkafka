package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical

/**
  * @author rdas2 on 9/16/2020
  *
  * */
case class DAQELoadedEvent(action: String,
                           eventType: String,
                           payload: PayloadData,
                           status: String,
                           ts: Long)


case class PayloadData(batchTable: String,
                       client:String,
                       kafkaPiggyBack: String,
                       layoutName: String,
                       runId: String,
                       tabToValidate: String,
                       targetSchema: String)

case class ComputerAttack(src_impl_pit_id: String,
                           src_proc_pit_id: String,
                           data_typ_shrt_nm: String,
                           impl_btch_pit_id: String)

case class fileStructure(ipAddress:String,noVal:String,http_add:String,description:String)

