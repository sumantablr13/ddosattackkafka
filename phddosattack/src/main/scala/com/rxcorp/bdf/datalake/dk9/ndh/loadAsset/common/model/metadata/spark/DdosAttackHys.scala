package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{SparkPartitionTable, SparkTable}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared.AbstractMDataKafkaOffset

class DdosAttackHys(appContext: AppContext)  extends AbstractDdosAttackHys(appContext) with  SparkTable {

}