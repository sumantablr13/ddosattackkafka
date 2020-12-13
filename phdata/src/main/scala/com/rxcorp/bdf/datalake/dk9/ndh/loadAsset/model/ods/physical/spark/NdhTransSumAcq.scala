package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared.AbstractNdhTransSumAcq

/**
  * @author rdas2 on 9/22/2020
  *
  * */
class NdhTransSumAcq(appContext: AppContext) extends AbstractNdhTransSumAcq(appContext) with SparkPartitionTable {

}
