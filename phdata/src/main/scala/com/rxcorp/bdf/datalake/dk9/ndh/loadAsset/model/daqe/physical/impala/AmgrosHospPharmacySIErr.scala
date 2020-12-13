package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.shared.AbstractAmgrosHospPharmacySIErr

/**
  * @author rdas2 on 10/4/2020
  *
  * */
class AmgrosHospPharmacySIErr(appContext: AppContext) extends AbstractAmgrosHospPharmacySIErr(appContext) with ImpalaPartitionTable {

}
