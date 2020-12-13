package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark

class TestMDataTableLoadLog(databaseName: String, testSuiteSuffix: String) extends MDataTblLoadLog(databaseName: String) {
  override lazy val physicalName: String = s"mdata_tbl_load_log_${testSuiteSuffix}"
}