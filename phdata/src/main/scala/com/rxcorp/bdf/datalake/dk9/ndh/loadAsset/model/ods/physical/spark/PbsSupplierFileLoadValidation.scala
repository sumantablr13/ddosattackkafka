package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared.AbstractPbsSupplierFileLoadValidation

class PbsSupplierFileLoadValidation(appContext: AppContext) extends AbstractPbsSupplierFileLoadValidation(appContext) with SparkPartitionTable{


}
