package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical

import java.sql.Timestamp

import spray.json._

case object OdsLoadedEventProtocol extends DefaultJsonProtocol {
  //implicit val supplierLoadedEventProtocol = jsonFormat4(OdsLoadedEvent)

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

  implicit case object OdsLoadedEventJsonFormat extends RootJsonFormat[OdsLoadedEvent] {

    def write(record: OdsLoadedEvent) = {
      JsObject(
        Map[String, JsValue](
          "src_impl_pit_id" -> handleNullNumber(record.src_impl_pit_id),
          "src_proc_pit_id" -> handleNullString(record.src_proc_pit_id),
          "data_typ_shrt_nm" -> handleNullString(record.data_typ_shrt_nm),
          "impl_btch_pit_id" -> handleNullString(record.impl_btch_pit_id),
          "supld_dsupp_proc_id" -> handleNullString(record.supld_dsupp_proc_id),
          "dervd_dsupp_file_freq_typ_cd" -> handleNullString(record.dervd_dsupp_file_freq_typ_cd),
          "dervd_dsupp_file_tm_pd_nbr" -> handleNullNumber(record.dervd_dsupp_file_tm_pd_nbr),
          "ops_tbl_isrted_ts" -> handleNullTimestamp(record.ops_tbl_isrted_ts),
          "ops_topic_isrted_ts" -> handleNullTimestamp(record.ops_topic_isrted_ts)
        )
      )
    }

    def read(value: JsValue) = {
      val jsonMap = value.asJsObject.fields
      OdsLoadedEvent(
        jsonMap.get("src_impl_pit_id").map(handleNullJsInteger).orNull.toShort,
        jsonMap.get("src_proc_pit_id").map(handleNullJsString).orNull,
        jsonMap.get("data_typ_shrt_nm").map(handleNullJsString).orNull,
        jsonMap.get("impl_btch_pit_id").map(handleNullJsString).orNull,
        jsonMap.get("supld_dsupp_proc_id").map(handleNullJsString).orNull,
        jsonMap.get("dervd_dsupp_file_freq_typ_cd").map(handleNullJsString).orNull,
        jsonMap.get("dervd_dsupp_file_tm_pd_nbr").map(handleNullJsInteger).orNull,
        jsonMap.get("ops_tbl_isrted_ts").map(handleNullJsTimestamp).orNull,
        jsonMap.get("ops_topic_isrted_ts").map(handleNullJsTimestamp).orNull
      )
    }
  }

}