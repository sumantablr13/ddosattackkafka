package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractTimeDyDim

class TimeDyDim(appContext: AppContext) extends AbstractTimeDyDim(appContext) with SparkTable {
}
