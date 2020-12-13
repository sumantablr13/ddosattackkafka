package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractLmsProduct

class LmsProduct(appContext: AppContext) extends AbstractLmsProduct(appContext) with ImpalaPartitionTable {
}