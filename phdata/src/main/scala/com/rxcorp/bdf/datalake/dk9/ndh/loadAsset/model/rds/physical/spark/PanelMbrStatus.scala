package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared.AbstractPanelMbrStatus

class PanelMbrStatus(appContext: AppContext) extends AbstractPanelMbrStatus(appContext) with SparkTable  {
}