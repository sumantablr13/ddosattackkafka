package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared.AbstractNdhFactOdsErrLog

/*
    Created by SMydur on 09/05/2018
 */

class NdhFactOdsErrLog(appContext: AppContext) extends AbstractNdhFactOdsErrLog(appContext) with SparkPartitionTable {
}
