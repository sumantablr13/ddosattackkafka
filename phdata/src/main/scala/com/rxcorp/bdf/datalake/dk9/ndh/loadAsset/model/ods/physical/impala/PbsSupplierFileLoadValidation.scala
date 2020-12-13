package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared.AbstractPbsSupplierFileLoadValidation

class PbsSupplierFileLoadValidation(appContext: AppContext) extends AbstractPbsSupplierFileLoadValidation(appContext) with ImpalaPartitionTable {
}