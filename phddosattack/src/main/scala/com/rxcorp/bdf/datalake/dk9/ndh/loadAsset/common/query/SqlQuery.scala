package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query

abstract class SqlQuery() {
  val logMessage: String
  val sqlStatement: String
}
