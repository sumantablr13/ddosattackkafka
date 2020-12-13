package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractLmsPack

/**
 * @author rdas2 on 8/26/2020
 *
 **/
class LmsDrugNames(appContext: AppContext) extends AbstractLmsPack(appContext) with ImpalaPartitionTable {
}
