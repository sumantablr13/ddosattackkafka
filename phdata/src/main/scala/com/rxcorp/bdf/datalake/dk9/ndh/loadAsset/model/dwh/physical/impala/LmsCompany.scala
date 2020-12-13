package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.impala

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.ImpalaPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.shared.AbstractLmsCompany

/**
 * @author rdas2 on 8/24/2020
 *
 **/
class LmsCompany(appContext: AppContext) extends AbstractLmsCompany(appContext) with ImpalaPartitionTable {
}
