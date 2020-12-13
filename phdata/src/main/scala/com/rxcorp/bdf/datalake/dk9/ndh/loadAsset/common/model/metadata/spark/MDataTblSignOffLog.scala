package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkPartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared.AbstractMDataTblSignOffLog

class MDataTblSignOffLog(appContext: AppContext) extends AbstractMDataTblSignOffLog(appContext) with SparkPartitionTable
