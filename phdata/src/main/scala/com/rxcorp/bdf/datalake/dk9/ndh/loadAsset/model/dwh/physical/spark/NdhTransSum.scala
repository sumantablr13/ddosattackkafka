package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractNdhTransSum

/**
  * @author rdas2 on 9/22/2020
  *
  * */
class NdhTransSum(appContext: AppContext) extends AbstractNdhTransSum(appContext) with SparkPartitionTable {

}