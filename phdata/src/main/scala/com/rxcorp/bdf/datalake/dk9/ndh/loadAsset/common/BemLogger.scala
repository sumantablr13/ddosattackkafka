package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import java.util.UUID

import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import org.json4s.{DefaultFormats, NoTypeHints}
import scalaj.http._

class BemLogger(businessRestUrl: String,
                applicationRestUrl: String,
                countryScope: String,
                assetTypeScope: String,
                logUser: String,
                componentLifecycle: String,
                bemLoggerTimeout: Integer
               ) {

  final val CFG_BUSINESS_REST_URL=businessRestUrl
  final val CFG_APPLICATION_REST_URL=applicationRestUrl
  final val CFG_COUNTRY_SCOPE=countryScope
  final val CFG_ASSET_TYPE_SCOPE=assetTypeScope
  final val CFG_LOG_USER = logUser
//  final val CFG_COMPONENT_IDENTIFIER = componentIdentifier
  final val CFG_COMPONENT_VERSION = "1.0.0"
  final val CFG_COMPONENT_LIFECYCLE = componentLifecycle.toLowerCase.capitalize  //e.g. Development

  protected var _dataName: String         = BemConf.UNDEFINED
  protected var _eventScope: String       = BemConf.UNDEFINED
  protected var _productionCycle: String  = BemConf.UNDEFINED
  protected var _correlationUUID: String  = generateUUID
  protected var _eventOperation: String   = BemConf.UNDEFINED
  protected var _eventId: String          = BemConf.UNDEFINED
  protected var _componentId: String      = "Process"
//  protected var _eventOutcome: String     = BemConf.UNDEFINED

  protected def generateUUID(): String = {
    UUID.randomUUID.toString.toUpperCase
  }
  protected def setDataName(dataName: String) = {
    _dataName = dataName
  }
  protected def setEventScope(eventScope: String) = {
    _eventScope = eventScope
  }
  protected def setProductionCycle(productionCycle: String) = {
    _productionCycle = productionCycle
  }
  protected def setEventId(eventType: String) = {
    if (eventType == BemConf.EventType.STARTED_EVENT) _eventId = generateUUID()
  }
  protected def setComponentId(componentId: String) = {
    if (componentId.equals("")) _componentId = getComponentId else  _componentId = componentId
  }
  protected def setCorrelationUUID(correlationUUID: String) = {
//    val uuid = correlationUUID match {case Some(value) => value case _ => BemConf.generateUUID}
    _correlationUUID = correlationUUID
  }
  protected def setEventOperation(eventOperation: String, eventType: String) = {
    if (eventType == BemConf.EventType.STARTED_EVENT) _eventOperation = eventOperation
  }

//  protected def setEventOutcome(eventOutcome: String, eventType: String) = {
//    _eventOutcome = {
//      if (eventType == BemConf.EventType.FINISHED_EVENT) eventOutcome
//      else BemConf.UNDEFINED
//    }
//  }

  def getCorrelationUUID = {
    _correlationUUID
  }
  def getDataName = {
    _dataName
  }
  def getEventScope = {
    _eventScope
  }
  def getProductionCycle = {
    _productionCycle
  }
  def getEventOperation = {
    _eventOperation
  }
  def getEventId = {
    _eventId
  }
  def getComponentId = {
    _componentId
  }
  def getSpanId(correlationUUID: String, eventDomain: String): String = {
    val spanIdCode = eventDomain match {
      case BemConf.EventDomain.DATA_EXTRACTION_COLLECTION => correlationUUID.takeRight(3).toString
      case BemConf.EventDomain.DATA_AQUISITION => "002"
      case BemConf.EventDomain.BRIDGING => "003"
      case BemConf.EventDomain.DATAWAREHOUSE => "004"
      case BemConf.EventDomain.REFERENCE_DATA => "005"
      case BemConf.EventDomain.DATA_CHANGE_CAPTURE => "006"
      case BemConf.EventDomain.DATA_QUALITY => "007"
      case BemConf.EventDomain.REPORT_DEFINITION_EXTRACTION => "008"
      case BemConf.EventDomain.BUSINESS_EVENT_BUS => "009"
      case _  => "000"
    }
    correlationUUID.replaceAll("...$", spanIdCode)
  }
  def getParentSpanId(correlationUUID: String, eventDomain: String): String = {
    eventDomain match {
      case BemConf.EventDomain.DATA_EXTRACTION_COLLECTION => correlationUUID
      case BemConf.EventDomain.DATA_AQUISITION => getSpanId(correlationUUID, BemConf.EventDomain.DATA_EXTRACTION_COLLECTION)
      case BemConf.EventDomain.BRIDGING => getSpanId(correlationUUID, BemConf.EventDomain.DATA_AQUISITION)
      case BemConf.EventDomain.DATAWAREHOUSE => getSpanId(correlationUUID, BemConf.EventDomain.BRIDGING)
      case BemConf.EventDomain.REFERENCE_DATA => getSpanId(correlationUUID, BemConf.EventDomain.DATAWAREHOUSE)
      case BemConf.EventDomain.DATA_CHANGE_CAPTURE => getSpanId(correlationUUID, BemConf.EventDomain.REFERENCE_DATA)
      case BemConf.EventDomain.DATA_QUALITY => getSpanId(correlationUUID, BemConf.EventDomain.DATA_CHANGE_CAPTURE)
      case BemConf.EventDomain.REPORT_DEFINITION_EXTRACTION => getSpanId(correlationUUID, BemConf.EventDomain.DATA_QUALITY)
      case BemConf.EventDomain.BUSINESS_EVENT_BUS => getSpanId(correlationUUID, BemConf.EventDomain.REPORT_DEFINITION_EXTRACTION)
    }
  }

  def setBemLoggerContext(correlationUUID: String,
                          dataName: String,
                          eventScope: String,
                          productionCycle: String) = {
    setCorrelationUUID(correlationUUID)
    setDataName(dataName)
    setEventScope(eventScope)
    setProductionCycle(productionCycle)
  }

  def logBusinessEventStart(eventDescription: String, eventOperation: String, componentIdentifier: String) = {
    log(
      eventDescription = eventDescription,
      eventOutcome = BemConf.EventOutcome.SUCCESS,
      eventType = BemConf.EventType.STARTED_EVENT,
      eventOperation = eventOperation,
      componentIdentifier = componentIdentifier
    )
  }

  def logBusinessEventFinish(eventDescription: String, eventOutcome: String) = {
    log(
      eventDescription = eventDescription,
      eventOutcome = eventOutcome,
      eventType = BemConf.EventType.FINISHED_EVENT
    )
  }

  protected def log(
          eventDescription: String,
          eventOutcome: String,
          eventType: String,
          eventOperation: String = getEventOperation,
          componentIdentifier: String = getComponentId,
          eventMode: String = BemConf.EventMode.BUSINESS_EVENT): Boolean = {

    val correlationUUID = getCorrelationUUID
    val eventTime = new TimestampConverter().tsUTCText
    val eventDomain = BemConf.getEventDomain(eventOperation)
    if (eventDomain.equals(BemConf.UNDEFINED))
      throw new IllegalArgumentException(s"PBS BEM Logger Internal Error. " +
        s"Incorrect or no input arguments passed for logger parameter eventOperation='${eventOperation}'")

    val dataName = getDataName
    val eventScope = getEventScope
    val productionCycle = getProductionCycle
    setEventOperation(eventOperation, eventType)
    val eventStartOperation = getEventOperation
    setEventId(eventType)
    val eventId = getEventId
    setComponentId(componentIdentifier)
    val componentId = getComponentId

    println("Sending data to REST End Point")
    if (eventMode == BemConf.EventMode.BUSINESS_EVENT) {
      val genJson: String = constructBusinessEventJSON(correlationUUID, eventTime, eventMode, eventDomain, eventStartOperation,
        eventType, eventOutcome, dataName, eventDescription, eventScope, productionCycle, eventId, componentId)
      println("Posting JSON Data for Business Event")
      println(genJson)
      postJsonData(genJson, CFG_BUSINESS_REST_URL)
    }
    else /*(eventMode == BemConf.BEM_EVENTMODE.APPLICATION_EVENT) */ {
      val genJson: String = constructApplicationEventJSON(correlationUUID, eventTime, eventDomain,
        eventType, dataName, eventDescription, eventScope, productionCycle, eventId, componentId,
        eventLevel = BemConf.EventLevel.INFO)
      println("Posting JSON Data for Application Event")
      println(genJson)
      postJsonData(genJson, CFG_APPLICATION_REST_URL)
    }
  }

  // Post REST API for BEM
  //@TODO: may be moved to executors as a separate executor for invoking REST calls
  def postJsonData(reqJson: String, postURL: String): Boolean = {

    // Added to fix BEM Production issue as Unsafe Security is not allowed in Prod BEM DCOS Server...
    val response = if (CFG_COMPONENT_LIFECYCLE.equals("Production")) {
      Http(postURL).postData(reqJson)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(bemLoggerTimeout)) // BEM_LOGGER_TIMEOUT
        .asString
    } else {
      Http(postURL).postData(reqJson)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(bemLoggerTimeout)) // BEM_LOGGER_TIMEOUT
        .option(HttpOptions.allowUnsafeSSL)
        .asString
    }

    println("Response Code : " + response.code)

    if (response.isSuccess) {
      println("Event posted successfully!")
      true
    }
    else if (response.isClientError) {
      // @TODO: - to discuss we want to throw an error
      //  (currently process will work even if the BEM rejects our message)
      //  validation on BEM side is not allowing UNITTEST passed for productionCycle,
      //  if we raise exception here, unit tests will fail
      println("Could not post the event due to client error :")
      println(response.body.toString)
      println("Committing the offset.")
      true
    }
    else {
      println("Event post error : " + response.body.toString)
      false
    }
  }

  def constructBusinessEventJSON(correlationUUID: String, eventTime: String, eventMode: String,
                                 eventDomain: String, eventOperation: String,
                                 eventType: String, eventOutcome: String,
                                 dataName: String, eventDescription: String,
                                 eventScope: String, productionCycle: String,
                                 eventId: String, componentId: String): String = {
//    val eventScopeAttrName = getEventScopeAttrName(eventDomain)
    val spanId = getSpanId(correlationUUID, eventDomain)
    val parentSpanId = getParentSpanId(correlationUUID, eventDomain)

    val jsonBusinessEvent = Map(
      "eventTimestamp" -> eventTime,
      "correlationUUID" -> correlationUUID,
      "eventDomain" -> eventDomain,
      "eventOperation" -> eventOperation,
      "eventType" -> eventType,
      "eventOutcome" -> eventOutcome,
      "eventDescription" -> eventDescription,
      "eventId" -> eventId,
      "spanId" -> spanId,
      "parentSpanId" -> parentSpanId,
      "componentIdentifier" -> componentId,
      "componentVersion" -> CFG_COMPONENT_VERSION,
      "componentLifecycle" -> CFG_COMPONENT_LIFECYCLE,
      "metaData" -> Map(
        "user" -> CFG_LOG_USER,
        "dataName" -> dataName,
        "countryScope" -> CFG_COUNTRY_SCOPE,
        "assetTypeScope" -> CFG_ASSET_TYPE_SCOPE,
        BemConf.getEventScopeAttrName(eventDomain) -> eventScope,
        "productionCycle" -> productionCycle
      )
    )
    implicit val formats = Serialization.formats(NoTypeHints)
    write(jsonBusinessEvent)
  }

  def constructApplicationEventJSON(correlationUUID: String, eventTime: String, eventDomain: String,
                                    eventType: String, dataName: String, eventDescription: String,
                                    eventScope: String, productionCycle: String, eventId: String,
                                    componentId: String, eventLevel: String): String = {

    val jsonApplicationEvent = Map(
      "eventTimestamp" -> eventTime,
      "correlationUUID" -> correlationUUID,
      "eventLevel"          -> eventLevel,  // only for app
      "eventType" -> eventType,
      "eventDescription" -> eventDescription,
      "eventId" -> eventId,
      "componentIdentifier" -> componentId,
      "componentVersion" -> CFG_COMPONENT_VERSION,
      "componentLifecycle" -> CFG_COMPONENT_LIFECYCLE,
      "metaData" -> Map(
        "user" -> CFG_LOG_USER,
        "dataName" -> dataName,
        "countryScope" -> CFG_COUNTRY_SCOPE,
        "assetTypeScope" -> CFG_ASSET_TYPE_SCOPE,
        BemConf.getEventScopeAttrName(eventDomain)  -> eventScope,
        //  "panelId"         -> metaData.bemAppPanelId,        NEED CLARITY
        "productionCycle" -> productionCycle
      )
    )
    implicit val formats = DefaultFormats
    write(jsonApplicationEvent)
  }

}

