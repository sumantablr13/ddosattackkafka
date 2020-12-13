package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.time.Instant
import java.sql.Timestamp

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._

class TimestampConverter(val ts: Timestamp = Timestamp.from(Instant.now())) {

  val millis: Long = ts.getTime
  val unixTime: Long = millis / 1000
  val sparkSqlUTCText: String = s"""from_unixtime(${unixTime})"""
  val sparkSqlUTCCol: Column = from_unixtime(lit(unixTime))

  // That function should be used as String representation of Timestamp columns (ex. in case classes)
  val tsUTCText: String = DateTimeUtils.newDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", TimeZone.getTimeZone("UTC")).format(ts)

  // Do not use below function for populating Hive Timestamp fields - these need to be populated with tsUTCText
  def notTsUTCText(format: String) = DateTimeUtils.newDateFormat(format, TimeZone.getTimeZone("UTC")).format(ts)

  def getMinus1Sec: TimestampConverter = new TimestampConverter(Timestamp.from(Instant.ofEpochMilli(millis-1000)))
}

object TimestampConverter {
  final val TIMESTAMP_MINUS_INFINITY = parse("1800-01-01T00:00:00.0+00:00","yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  final val TIMESTAMP_PLUS_INFINITY = parse("2999-12-31T00:00:00.0+00:00","yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  final val DEFAULT_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
  final val DEFAULT_TS_WITH_TZ_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

  final def parse(inputString: String, inputDateFormat: String): TimestampConverter = {
    val sdf = new SimpleDateFormat(inputDateFormat)
    sdf.setLenient(false)
    new TimestampConverter(Timestamp.from(Instant.ofEpochMilli(sdf.parse(inputString).getTime)))
  }
}
