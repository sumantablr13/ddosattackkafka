package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractDspnsdRxTrans

class DspnsdRxTrans(appContext: AppContext) extends AbstractDspnsdRxTrans(appContext) with ImpalaPartitionTable {
}