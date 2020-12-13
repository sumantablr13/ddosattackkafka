package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared.AbstractNdhFactOdsErrLog

/*
    Created by SMydur on 09/05/2018
 */

class NdhFactOdsErrLog(appContext: AppContext) extends AbstractNdhFactOdsErrLog(appContext) with ImpalaPartitionTable {
}
