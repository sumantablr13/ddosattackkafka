package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.{SparkPartitionTable, SparkTable}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared.AbstractMDataKafkaOffset

class MDataKafkaOffset(appContext: AppContext) extends AbstractMDataKafkaOffset(appContext) with SparkTable
