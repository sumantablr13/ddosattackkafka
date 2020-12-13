package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import org.apache.spark.sql.DataFrame
import org.joda.time.{DateTime, IllegalFieldValueException}

object DayWeekYearFinder {

  def getWeekNumber(yearNum: String,appContext: AppContext,date:DateTime): Int = {

    val weekNumber = 0
    try {
      var year = getStdYear(yearNum,appContext)
      val weekNumberRng: Int =date.withYear(year.toInt).weekOfWeekyear().getMaximumValue
      weekNumberRng
    } catch {
      case ife: IllegalFieldValueException => {
        weekNumber
      }
      case ex: Exception => {
        weekNumber
      }
    }
  }

  def getMonthWeekNumber(ds: DataFrame, columnName: Seq[String],appContext: AppContext):Int={
    var dayNumber = 0
    try {
      val rows = ds.select(ds.col(columnName(0)), ds.col(columnName(1)),ds.col(columnName(2))).collect()
      var year = getStdYear(rows(0).getString(0),appContext)
      var month = rows(0).getString(1)
      var day=rows(0).getString(2)
      dayNumber= new DateTime().withYear(year.toInt).withMonthOfYear(month.toInt).withDayOfMonth(day.toInt).weekOfWeekyear().get()
      dayNumber
    }catch {
      case e:Exception=>{
        dayNumber
      }
    }
  }
  def getDayNumber(yearNum:String,monthNum:String,appContext: AppContext,dateTime:DateTime): Int = {
    val dayNumberRng = 0
    try {
      val year = getStdYear(yearNum,appContext)
      val month = monthNum
      val dayNumber = dateTime.withYear(year.toInt).withMonthOfYear(month.toInt).dayOfMonth().getMaximumValue
      dayNumber
    } catch {
      case ife: IllegalFieldValueException => {
        dayNumberRng
      }
      case ex: Exception => {
        dayNumberRng
      }
    }
  }

  def getStdYear(year:String,appContext: AppContext):String={
    try {
      var stdYear = year;
      if (stdYear.length <= 2) {
        stdYear = "20" + year
      }
      stdYear
    }catch {
      case e:Exception=>{
        ""
      }
    }
  }
}
