package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.SparkExecutor

trait SparkServiceTrait extends AbstractServiceTrait {
  lazy val sparkExecutor: SparkExecutor = appContext.getSparkExecutor(appContext.config)

}
