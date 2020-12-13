package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractLmsPack

/**
 * @author rdas2 on 8/24/2020
 *
 **/
class LmsPack(appContext: AppContext) extends AbstractLmsPack(appContext) with SparkPartitionTable {
}