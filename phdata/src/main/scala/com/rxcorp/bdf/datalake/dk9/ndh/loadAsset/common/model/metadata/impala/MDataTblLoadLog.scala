package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared.AbstractMDataTblLoadLog

class MDataTblLoadLog(database: String) extends AbstractMDataTblLoadLog(database) with ImpalaPartitionTable