package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{SparkPartitionTable, SparkTable}

class DdosAttackHysT(appContext: AppContext)  extends AbstractDdosAttackHysT(appContext) with  SparkPartitionTable {

}
