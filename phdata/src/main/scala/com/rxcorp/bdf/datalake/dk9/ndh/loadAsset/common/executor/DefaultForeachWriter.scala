package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

/**
  * Created by DJha on 21/03/2018.
  */
import org.apache.spark.sql.ForeachWriter

abstract class DefaultForeachWriter[T] extends ForeachWriter[T]{
  override def open(partitionId: Long, version: Long): Boolean = true
  override def close(errorOrNull: Throwable): Unit = {}
}

