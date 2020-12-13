package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractTimeWkDim

class TimeWkDim(appContext: AppContext) extends AbstractTimeWkDim(appContext) with SparkTable {
}
