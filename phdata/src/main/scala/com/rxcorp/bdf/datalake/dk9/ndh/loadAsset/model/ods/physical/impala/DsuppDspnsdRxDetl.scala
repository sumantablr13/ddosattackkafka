package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared.{AbstractDsuppDspnsdRxDetl}

/**
  * Created by sravani vitta on 19/08/2019
  *
  * */

class DsuppDspnsdRxDetl(appContext: AppContext) extends AbstractDsuppDspnsdRxDetl(appContext) with ImpalaPartitionTable {
}