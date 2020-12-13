

package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared.{AbstractDsuppDspnsdRxDetl}

/*
    Created by Sravani Vitta on 19/08/2019
 */

class DsuppDspnsdRxDetl(appContext: AppContext) extends AbstractDsuppDspnsdRxDetl(appContext) with SparkPartitionTable {
}
