package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractLmsCompany

/**
 * @author rdas2 on 8/24/2020
 *
 **/
class LmsCompany(appContext: AppContext) extends AbstractLmsCompany(appContext) with SparkPartitionTable {
}
