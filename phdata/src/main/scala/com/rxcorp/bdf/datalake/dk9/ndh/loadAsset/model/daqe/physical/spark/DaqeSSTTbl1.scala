package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.shared.AbstractDaqeSSTTbl1

/**
  * @author rdas2 on 9/25/2020
  *
  * */
class DaqeSSTTbl1(appContext: AppContext) extends AbstractDaqeSSTTbl1(appContext) with SparkPartitionTable {

}
