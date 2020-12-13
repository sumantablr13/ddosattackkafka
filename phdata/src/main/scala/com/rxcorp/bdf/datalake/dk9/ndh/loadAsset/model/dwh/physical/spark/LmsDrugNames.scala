package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractLmsDrugNames

/**
 * @author rdas2 on 8/26/2020
 *
 **/
class LmsDrugNames(appContext: AppContext) extends AbstractLmsDrugNames(appContext) with SparkPartitionTable {
}
