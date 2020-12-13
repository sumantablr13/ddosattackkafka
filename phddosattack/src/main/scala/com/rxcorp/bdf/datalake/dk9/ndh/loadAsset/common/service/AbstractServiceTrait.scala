package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext

trait AbstractServiceTrait
{
  lazy val appContext: AppContext = AppContext()
  def main(args: Array[String])
}