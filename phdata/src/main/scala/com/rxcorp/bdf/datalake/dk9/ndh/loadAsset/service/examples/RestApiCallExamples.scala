package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.examples

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service.{AbstractServiceTrait, SparkServiceTrait}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.dwh.ReadApprovedCC
import org.apache.commons.lang3.exception.ExceptionUtils

/**
  * Created by Brahmdev Kumar on 03/18/2020.
  */
object RestApiCallExamples extends RestApiCallExamplesTrait {
}

trait RestApiCallExamplesTrait extends AbstractServiceTrait with SparkServiceTrait {

  def actionOnEventFailed(e: Exception): Unit = {
    val retStack = e.toString + " --> " + ExceptionUtils.getStackTrace(e)
  }

  def processOptions(): Unit = {

    try {
      val readCorrCard = ReadApprovedCC(appContext,sparkExecutor)

      //card level info
      //val cardLeveldf = readCorrCard.getCardLevelInfo("PROD", "rest_api_mock_001.csv" )
     // cardLeveldf.show(false)
      //rule level  info
     // val ruleLeveldf = readCorrCard.getRuleLevelInfo("PROD", "rest_api_mock_001.csv" )
     // ruleLeveldf.show(false)
    }
    catch {
      case e: Exception =>
        actionOnEventFailed(e)
        e.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {

    processOptions()

  }
}
