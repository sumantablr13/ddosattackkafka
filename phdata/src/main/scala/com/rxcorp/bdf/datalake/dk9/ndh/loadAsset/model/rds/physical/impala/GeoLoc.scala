package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared.AbstractGeoLoc

class GeoLoc(appContext: AppContext) extends AbstractGeoLoc(appContext) with ImpalaTable {
}