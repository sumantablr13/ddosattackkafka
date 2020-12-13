package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical

import java.sql.Timestamp

import spray.json._

/**
  * @author rdas2 on 9/16/2020
  *
  * */
case object SrcKafkaEventProtocol extends DefaultJsonProtocol {
  def handleNullString(value: String): JsValue = {
    Option(value).map(str => JsString(str)).getOrElse(JsNull)
  }

  def handleNullNumber(value: Integer): JsValue = {
    Option(value).map(number => JsNumber(number)).getOrElse(JsNull)
  }

  def handleNullNumber(value: Long): JsValue = {
    Option(value).map(number => JsNumber(number)).getOrElse(JsNull)
  }

  def handleNullTimestamp(value: Timestamp): JsValue = {
    Option(value).map(time => JsNumber(time.getTime)).getOrElse(JsNull)
  }

  def handleNullJsString(jsValue: JsValue): String = {
    if (jsValue != null && jsValue.getClass.equals(classOf[JsString])) {
      jsValue.asInstanceOf[JsString].value
    } else {
      null
    }
  }

  def handleNullJsInteger(jsValue: JsValue): Integer = {
    if (jsValue != null && jsValue.getClass.equals(classOf[JsNumber])) {
      jsValue.asInstanceOf[JsNumber].value.toInt
    } else {
      null
    }
  }

  def handleNullJsTimestamp(jsValue: JsValue): Timestamp = {
    if (jsValue != null && jsValue.getClass.equals(classOf[JsNumber])) {
      new Timestamp(jsValue.asInstanceOf[JsNumber].value.toLong)
    } else {
      null
    }
  }

  implicit case object PayloadDataFormat extends RootJsonFormat[PayloadData] {
    def write(record: PayloadData): JsObject =
      JsObject(
        Map[String, JsValue](
          "batchTable" -> record.batchTable.toJson,
          "client" -> record.client.toJson,
          "kafkaPiggyBack" -> record.kafkaPiggyBack.toJson,
          "layoutName" -> record.layoutName.toJson,
          "runId" -> record.runId.toJson,
          "tabToValidate" -> record.tabToValidate.toJson,
          "targetSchema" -> record.targetSchema.toJson
        ))

    def read(value: JsValue): PayloadData = {
      val jsonMap = value.asJsObject.fields
      PayloadData(
        jsonMap("batchTable").convertTo[String],
        jsonMap("client").convertTo[String],
        jsonMap("kafkaPiggyBack").convertTo[String],
        jsonMap("layoutName").convertTo[String],
        jsonMap("runId").convertTo[String],
        jsonMap("tabToValidate").convertTo[String],
        jsonMap("targetSchema").convertTo[String]
      )
    }
  }

  /*implicit case object PiggyBackFormat extends RootJsonFormat[PiggyBackData] {
    def write(record: PiggyBackData): JsObject =
      JsObject(
        Map[String, JsValue]()

    def read(value: JsValue): PiggyBackData = {
      val jsonMap = value.asJsObject.fields
      PiggyBackData(
        jsonMap("Supplier").convertTo[String],
        jsonMap("FileName").convertTo[String],
        jsonMap("FilePeriodLevel").convertTo[String],
        jsonMap("FilePeriod").convertTo[String],
        jsonMap("FileType").convertTo[String]
      )
    }
  }*/

  implicit case object DAQEEventFormat extends RootJsonFormat[DAQELoadedEvent] {
    def write(record: DAQELoadedEvent): JsObject =
      JsObject(
        Map[String, JsValue](
          "action" -> record.action.toJson,
          "eventType" -> record.eventType.toJson,
          "payload" -> record.payload.toJson,
          "status" -> record.status.toJson,
          "ts" -> record.ts.toJson
        ))

    def read(value: JsValue): DAQELoadedEvent = {
      val jsonMap = value.asJsObject.fields
      DAQELoadedEvent(
        jsonMap("action").convertTo[String],
        jsonMap("eventType").convertTo[String],
        jsonMap("payload").convertTo[PayloadData],
        jsonMap("status").convertTo[String],
        jsonMap("ts").convertTo[Long]
      )
    }
  }
  implicit case object ComputerAttackFormat extends RootJsonFormat[ComputerAttack] {
    def write(record: ComputerAttack): JsObject =
      JsObject(
        Map[String, JsValue](
          "src_impl_pit_id" -> record.src_impl_pit_id.toJson,
          "src_proc_pit_id" -> record.src_proc_pit_id.toJson,
          "data_typ_shrt_nm" -> record.data_typ_shrt_nm.toJson,
          "impl_btch_pit_id" -> record.impl_btch_pit_id.toJson
        ))

    def read(value: JsValue): ComputerAttack = {
      val jsonMap = value.asJsObject.fields
      ComputerAttack(
        jsonMap("src_impl_pit_id").convertTo[String],
        jsonMap("src_proc_pit_id").convertTo[String],
        jsonMap("data_typ_shrt_nm").convertTo[String],
        jsonMap("impl_btch_pit_id").convertTo[String]
      )
    }
  }



}
