package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import java.util.UUID

import com.rxcorp.bdf.logging.common.CommonUtils
import com.rxcorp.bdf.logging.internal.StandardImsLogger
import com.rxcorp.bdf.logging.log.{dataName, eventType, stepDescription, _}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.Logger


class AppLogger(logger: Logger) extends StandardImsLogger(logger: Logger) {

  final val statusSuccess = "SUCCESS"
  final val statusFailure = "FAILURE"
  final val statusWarning = "WARNING"
  final val withoutStep   = "WITHOUT_STEP"

  private def getCurrentTimestamp: String = {
    return new TimestampConverter().tsUTCText
  }

  private def getDefaultVersionLong: String = {
    return "1.0.0"
  }

  private def getDefaultVersionShort: String = {
    return getDefaultVersionLong.substring(0, 2).concat("x") //1.x
  }

  private def getDefaultStepType: String = {
    return "Rule"
  }

  private def getStepPathStage(stageNameVal: String,
                               stageNumber: Int,
                               stepFlowNumber: Int = 0): String = {
    return s"/${stepFlowNumber}:${stageNumber}|Some(${stageNameVal}-${getDefaultVersionShort})"
  }

  private def getStepPathRule(stageNameVal: String,
                              ruleNameVal: String,
                              stageNumber: Int,
                              ruleNumber: Int,
                              stepFlowNumber: Int = 0
                             ): String = {
    return s"${stepFlowNumber}:${stageNumber}|Some(${stageNameVal}-${getDefaultVersionShort})" +
      s"/${stepFlowNumber}:${ruleNumber}|Some(${ruleNameVal}-${getDefaultVersionShort})"
  }

  private def getStepPathStep(stageNameVal: String,
                              ruleNameVal: String,
                              stepNameVal: String,
                              stageNumber: Int,
                              ruleNumber: Int,
                              stepNumber: Int,
                              stepFlowNumber: Int = 0
                             ): String = {
    return s"${stepFlowNumber}:${stageNumber}|Some(${stageNameVal}-${getDefaultVersionShort})" +
      s"/${stepFlowNumber}:${ruleNumber}|Some(${ruleNameVal}-${getDefaultVersionShort})" +
      s"/${stepFlowNumber}:${stepNumber}|Some(${stepNameVal}-${getDefaultVersionShort})"
  }

  //@TODO: To think about introducing XMLs / Case Classes / JSON Files to store structures of the pipelines
  //       This way we could simply pass case class definition and offset for the step being logged
  //val pipeline =
  //  <pipeline pipelineName="pipelineName">
  //    <flow flowNumber="0">
  //      <stage stageName="FirstStage" stageNumber="0">
  //        <rule stepName="FirstRule" stepNumber="0"></rule>
  //        <rule stepName="SecondRule" stepNumber="1"></rule>
  //      </stage>
  //      <stage stageName="SecondStage" stageNumber="1">
  //        <rule stepName="SecondRule" stepNumber="0"></rule>
  //      </stage>
  //    </flow>
  //  </pipeline>

  def startPipeline(pipelineNameVal: String,
                    supplierScopeVal: String = "-",
                    dataNameVal: String = "-",
                    dataTypeVal: String = "-",
                    uniqNo: String = UUID.randomUUID().toString,
                    correlationUuid: Option[String] = None): Unit = {

    val eventMessage = s"Starting pipeline"
    val currentTimestamp = new TimestampConverter().tsUTCText
    this.add((pipelineName, pipelineNameVal))
    this.add((pipelineVersion, getDefaultVersionLong))
    this.add((supplierScope, supplierScopeVal))
    this.add((eventType, s"${eventMessage}"))
    this.add((eventTimestamp, currentTimestamp))
    this.add((startTime, currentTimestamp))
    this.add((dataName, dataNameVal))
    this.add((dataType, dataTypeVal))
    this.remove(eventOutcome)
    this.remove(endTime)
    this.remove(stageName)
    this.remove(stageVersion)
    this.remove(stepName)
    this.remove(stepVersion)
    this.remove(stepType)
    this.remove(stepPath)
    this.add((uniqueRequestId,uniqNo))
    this.add((correlationUUID, correlationUuid.getOrElse(uniqNo)))
    this.remove(tags)
    val logMessage = s"${eventMessage} ${pipelineNameVal}"
    this.module(s"${pipelineNameVal}")
    this.info(s"${logMessage}")
  }

  def startStage(stageNameVal: String,
                 stageNumber: Int,
                 stepFlowNumber: Int = 0,
                 stageDescriptionVal:String=""
                ): String = {

    //    0:0|Some(StageOne-1.x)/0:0|Some(StepOne-1.x)
    val eventMessage = s"Starting stage"
    val stepPathFinal = getStepPathStage(stageNameVal, stageNumber, stepFlowNumber)

    this.add((eventType, s"${eventMessage}"))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((startTime, getCurrentTimestamp))
    this.add((stageName, stageNameVal))
    this.add((stageVersion, getDefaultVersionLong))
    this.add((stepName, stageNameVal))
    this.add((stepDescription,stageDescriptionVal))/**Added by nshanmugam to show stage description*/
    this.add((stepVersion, getDefaultVersionLong))
    this.add((stepType, getDefaultStepType))
    this.add((stepPath, stepPathFinal))
    this.remove(eventOutcome)
    this.remove(endTime)
    this.remove((dataName))
    this.remove(tags)
    this.info(s"${eventMessage}")

    return stepPathFinal
  }

  def startRule(parentstageNameVal: String,
                ruleNameVal: String,
                parentStageNumber: Int,
                ruleNumber: Int,
                ruleDescriptionVal: String = "",
                stepFlowNumber: Int = 0
               ): String = {

    val eventMessage = s"Starting rule"
    //    0:0|Some(StageOne-1.x)/0:0|Some(RuleOne-1.x)
    val stepPathFinal =  getStepPathRule(parentstageNameVal,
      ruleNameVal,
      parentStageNumber,
      ruleNumber,
      stepFlowNumber)

    this.add((eventType, s"${eventMessage}"))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((startTime, getCurrentTimestamp))
    this.add((stageName, parentstageNameVal))
    this.add((stageVersion, getDefaultVersionLong))
    this.add((stepName, ruleNameVal))
    this.add((stepVersion, getDefaultVersionLong))
    this.add((stepType, getDefaultStepType))
    this.add((stepPath, stepPathFinal))
    this.add((stepDescription, ruleDescriptionVal))
    this.remove(eventOutcome)
    this.remove(endTime)
    this.remove((dataName))
    this.remove(tags)
    this.info(s"${eventMessage}")

    return stepPathFinal
  }

  def startStep(parentStageNameVal: String,
                parentRuleNameVal: String,
                stepNameVal: String,
                parentStageNumber: Int,
                parentRuleNumber: Int,
                stepNumber: Int,
                stepDescriptionVal: String = "",
                stepTypeVal: String = "transformation",
                stepFlowNumber: Int = 0
               ): String = {


    val eventMessage = s"Starting step"
    //    0:0|Some(StageOne-1.x)/0:0|Some(RuleOne-1.x)/0:0|Some(StepOne-1.x)
    val stepPathFinal =  getStepPathStep(parentStageNameVal,
      parentRuleNameVal,
      stepNameVal,
      parentStageNumber,
      parentRuleNumber,
      stepNumber,
      stepFlowNumber)

    this.add((eventType, s"${eventMessage}"))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((startTime, getCurrentTimestamp))
    this.add((stageName, parentStageNameVal))
    this.add((stageVersion, getDefaultVersionLong))
    this.add((stepName, stepNameVal))
    this.add((stepVersion, getDefaultVersionLong))
    this.add((stepType, stepTypeVal))
    this.add((stepPath, stepPathFinal))
    this.add((stepDescription, stepDescriptionVal))
    this.remove(eventOutcome)
    this.remove(endTime)
    this.remove((dataName))
    this.remove(tags)
    this.info(s"${eventMessage}")

    return stepPathFinal
  }

  def finishPipeline(eventOutcomeVal: String = statusSuccess, eventDescriptionVal: String = "", stackTraceVal: String = ""): Unit = {

    val eventMessage = s"Finishing pipeline"

    this.add((eventType, s"${eventMessage}"))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((endTime, getCurrentTimestamp))
    this.add((eventOutcome, s"${eventOutcomeVal}"))
    this.remove(tags)
    val logMessage = s"${eventMessage} with status ${eventOutcomeVal}"

    if (eventOutcomeVal.equals(statusFailure)) {
      this.add((eventDescription, eventDescriptionVal))
      this.add((stackTrace, stackTraceVal))
      this.error(s"${logMessage}")
    } else if (eventOutcomeVal.equals(statusWarning)) {
      this.warn(s"${logMessage}")
    } else {
      this.info(s"${logMessage}")
    }

  }

  def finishStage(stepPathVal: String,
                  eventOutcomeVal: String = statusSuccess,
                  dataTypeVal: String = "",
                  dataNameVal: String = "",
                  recordCountVal: String = "",
                  eventDescriptionVal: String = "",
                  stackTraceVal: String = "",
                  stageDescriptionVal:String=""): Unit = {

    val eventMessage = s"Finishing Stage"

    this.add((eventType, s"${eventMessage}"))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((endTime, getCurrentTimestamp))
    this.add((eventOutcome, s"${eventOutcomeVal}"))
    this.add((stepPath, stepPathVal))
    this.add((dataType, dataTypeVal))
    this.add((dataName, dataNameVal))
    this.add((stepType, getDefaultStepType))
    this.add((stepDescription,stageDescriptionVal))
    this.remove(tags)
    val logMessage = s"${eventMessage} with status ${eventOutcomeVal}"
    if (eventOutcomeVal.equals(statusFailure)) {
      this.add((eventDescription, eventDescriptionVal))
      this.add((stackTrace, stackTraceVal))
      this.error(s"${logMessage}")
    } else if (eventOutcomeVal.equals(statusWarning)) {
      this.warn(s"${logMessage}")
    } else {
      this.info(s"${logMessage}")
    }

    this.remove((dataType))
    this.remove((dataName))

  }

  def finishRule(stepPathVal: String = "",
                 eventOutcomeVal: String = statusSuccess,
                 dataTypeVal: String = "",
                 dataNameVal: String = "",
                 recordCountVal: String = "",
                 eventDescriptionVal: String = "",
                 stackTraceVal: String = "",
                 ruleDescriptionVal:String=""): Unit = {

    val eventMessage = s"Finishing rule"

    this.add((eventType, s"${eventMessage}"))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((endTime, getCurrentTimestamp))
    this.add((eventOutcome, s"${eventOutcomeVal}"))
    this.add((recordCount, recordCountVal))
    this.add((stepPath, stepPathVal))
    this.add((dataType, dataTypeVal))
    this.add((dataName, dataNameVal))
    this.add((stepType, getDefaultStepType))
    this.add((stepDescription,ruleDescriptionVal)) /*Added by nshanmugam*/
    this.remove(tags)
    val logMessage = s"${eventMessage} with status ${eventOutcomeVal}"
    if (eventOutcomeVal.equals(statusFailure)) {
      this.add((eventDescription, eventDescriptionVal))
      this.add((stackTrace, stackTraceVal))
      this.error(s"${logMessage}")

    } else if (eventOutcomeVal.equals(statusWarning)) {
      this.warn(s"${logMessage}")
    } else {
      this.info(s"${logMessage}")
    }
    this.remove((dataType))
    this.remove((dataName))
  }

  def finishStep(stepPathVal: String = "",
                 eventOutcomeVal: String = statusSuccess,
                 dataTypeVal: String = "",
                 dataNameVal: String = "",
                 recordCountVal: String = "",
                 eventDescriptionVal: String = "",
                 stackTraceVal: String = ""): Unit = {

    val eventMessage = s"Finishing Step"

    this.add((eventType, s"${eventMessage}"))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((endTime, getCurrentTimestamp))
    this.add((eventOutcome, s"${eventOutcomeVal}"))
    this.add((recordCount, recordCountVal))
    this.add((stepPath, stepPathVal))
    this.add((dataType, dataTypeVal))
    this.add((dataName, dataNameVal))
    this.remove(tags)
    val logMessage = s"${eventMessage} with status ${eventOutcomeVal}"
    if (eventOutcomeVal.equals(statusFailure)) {
      this.add((eventDescription, eventDescriptionVal))
      this.add((stackTrace, stackTraceVal))
      this.error(s"${logMessage}")

    } else if (eventOutcomeVal.equals(statusWarning)) {
      this.warn(s"${logMessage}")
    } else {
      this.info(s"${logMessage}")
    }
    this.remove((dataType))
    this.remove((dataName))
  }

  def logMessage(logMessage: String, detailedInfo: String = ""): Boolean = {
    this.add((eventType, logMessage))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.info(s"${detailedInfo}")
    return true
  }


  /**
    * This method used to create snapshot of mdata table.
    **/
  def logMetadata(fileNameVal:String,methodNameVal:String,recordCountVal:String,fileSize:String,insertedDate:String):Boolean={
    this.add((fileName,fileNameVal))
    this.add((methodName,methodNameVal))
    this.add((recordCount,recordCountVal))
    this.add((value,fileSize))
    this.add((logTimestamp,insertedDate))
    this.add((eventTimestamp,getCurrentTimestamp))
    this.info(s"Logging Messages to error log table")
    return true
  }

  def removeMetadataLogs():Boolean={
    this.remove(eventType)
    this.remove(fileName)
    this.remove(methodName)
    this.remove(recordCount)
    this.remove(value)
    this.remove(logTimestamp)
    this.info(s"Removing attributes...")
    return false
  }

  def logErrorPercentagedata(fileNameVal:String,recordCountVal:String,errRecordCount:String,percentage:String):Boolean={
    this.add((fileName,fileNameVal))
    this.add((recordCount,recordCountVal))
    this.add((value,errRecordCount))
    this.add((eventType,"ERROR_PRTG"))
    this.add((eventTimestamp,getCurrentTimestamp))
    this.info(percentage)
    return true
  }

  def removeErrorPercentagedata():Boolean={
    this.remove(eventType)
    this.remove(fileName)
    this.remove(recordCount)
    this.remove(value)
    this.info(s"Removing attributes...")
    return false
  }

  def logError(e: Exception): Boolean = {
    this.add((eventType, e.toString()))
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((stackTrace, e.toString()))
    this.error(ExceptionUtils.getStackTrace(e))
    return true
  }




  def qc0Logger(eventMessage: String = "",
                componentMetricNameVal: String,
                metricVal: Long = 0,
                tags: Map[String,String],
                eventOutcomeVal: String = "failure"
               ) = {
    val stepPathFinal = ""
    this.add((eventTimestamp, getCurrentTimestamp))
    this.add((componentMetricName, s"${componentMetricNameVal}"))
    this.add((value, s"${metricVal}"))
    this.add((eventOutcome, s"${eventOutcomeVal.toLowerCase}"))
    this.add((pipelineName, "QC0Pipeline"))
    this.addTags(tags)
    this.remove((stepName))
    this.remove((stepPath))
    this.remove((eventType))
    this.remove((scalaVersion))
    this.remove((loggerArtifactId))
    this.remove((loggerGroupId))
    this.remove((loggerVersion))
    this.remove((eventLevel))
    this.remove((user))
    this.remove((status))
    this.remove((componentAddress))
    this.remove((endTime))
    this.remove((stepName))
    this.remove((stageName))
    this.remove((stepPath))
    this.remove((stepLifecycle))
    this.remove((supplierScope))
    this.remove((stageLifecycle))
    this.remove((stepLifecycle))
    this.remove((stageVersion))
    this.remove((stepVersion))
    this.remove((pipelineName))
    this.remove((pipelineVersion))
    this.remove((eventType))
    this.remove((startTime))
    this.remove((endTime))
    //this.remove((eventOutcome))
    this.remove((dataName))
    this.remove((dataType))
    this.remove((stepType))
    this.remove((assetTypeScope))
    this.remove((stepDescription))
    //this.remove((pipelineLifecycle))
    this.remove((recordCount))
    this.remove((countryScope))
    //this.remove((eventOutcome))
    this.info(s"${eventMessage}")
  }

}

object AppLogger {
  val DEFAULT_STEP_TYPE = "transformation"

  def getLogger(loggerName:String): AppLogger = {
    CommonUtils.setProperties()
    //new AppLogger(Logger.getLogger(getClass.getName))
    new AppLogger(Logger.getLogger(loggerName))
  }
}
