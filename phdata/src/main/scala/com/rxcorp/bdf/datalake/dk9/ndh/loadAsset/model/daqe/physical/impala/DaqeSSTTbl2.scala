package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.shared.AbstractDaqeSSTTbl2

/**
  * @author rdas2 on 9/10/2020
  *
  **/
class DaqeSSTTbl2(appContext: AppContext) extends AbstractDaqeSSTTbl2(appContext) with ImpalaPartitionTable {

}
