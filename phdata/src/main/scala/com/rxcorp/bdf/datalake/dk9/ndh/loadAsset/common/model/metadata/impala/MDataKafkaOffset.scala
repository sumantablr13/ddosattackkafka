package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared.AbstractMDataKafkaOffset

class MDataKafkaOffset(appContext: AppContext) extends AbstractMDataKafkaOffset(appContext) with ImpalaPartitionTable