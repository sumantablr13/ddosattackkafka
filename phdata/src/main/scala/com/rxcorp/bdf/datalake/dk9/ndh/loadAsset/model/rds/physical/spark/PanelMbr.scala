package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared.AbstractPanelMbr

class PanelMbr(appContext: AppContext) extends AbstractPanelMbr(appContext) with SparkTable  {
}