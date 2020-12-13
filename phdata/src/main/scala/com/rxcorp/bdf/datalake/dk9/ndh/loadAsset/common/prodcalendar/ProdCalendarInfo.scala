package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.prodcalendar

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.SparkExecutor
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataPrdCyc
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SparkSqlQuery
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark.{TimeDyDim, TimeMthDim, TimeWkDim}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

class  ProdCalendarInfo( appContext: AppContext, sparkExecutor: SparkExecutor) {

  private val prodWeekStartDay: String= appContext.config.getString("PRD_WEEK_START_DAY")
  private val currentTs = new TimestampConverter()
  private val timeDyDim = new TimeDyDim(appContext) //Time Dimension (Day)
  private val timeWkDim = new TimeWkDim(appContext) //Time Dimension (Week)
 private val timeMthDim = new TimeMthDim(appContext) //Time Dimension (Month)
  private val mDataPrdCyc = new MDataPrdCyc(appContext) //mData Table
  private val startIndex: Int = 0
  private val endIndex: Int = 6
  private val weekDays: Int = -7
  private val sysEffTs = new TimestampConverter()
  private val asset_cd = appContext.config.getString("ENV_APPLICATION_CODE").toUpperCase.trim

  //returns production week start date for given period Number
  def getWeeklyPeriodStartDate(WeekNo: Int): String = {
    val startDate = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Production Week Start Date by given Week Number"
      override val sqlStatement: String =
        s"""
           |SELECT wk_strt_dt
           |FROM ${timeWkDim.tableName}
           |WHERE UPPER(wk_strt_day_nm) = UPPER('$prodWeekStartDay')
           |  AND dspsnd_rx_prd_yr_wk_nbr = $WeekNo
         """.stripMargin
    })

    Try(startDate.select("wk_strt_dt").head().getString(0)).getOrElse("")
  }

  //returns production week End date for given period Number
  def getWeeklyPeriodEndDate(WeekNo: Int): String = {
    val startDate = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Production Week End Date by given Week Number"
      override val sqlStatement: String =
        s"""
           |SELECT wk_end_dt
           |FROM ${timeWkDim.tableName}
           |WHERE UPPER(wk_strt_day_nm) = UPPER('$prodWeekStartDay')
           |   AND dspsnd_rx_prd_yr_wk_nbr = $WeekNo
         """.stripMargin
    })

    Try(startDate.select("wk_end_dt").head().getString(0)).getOrElse("")
  }

  // no parameters required, returns Current production weekid e.g. 20195026 based on current timestamp
  def getCurrentProdWeekPeriodId(daysAdd: Int = 0): Int = {
    val currentProdWeekDf = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Production week info by current date"
      override val sqlStatement: String =
        s"""
           |SELECT
           |  tm_pd_id
           |FROM ${timeWkDim.tableName}
           |WHERE UPPER(wk_strt_day_nm) = UPPER('$prodWeekStartDay')
           |  AND DATE_ADD(${currentTs.sparkSqlUTCText}, $daysAdd) BETWEEN wk_strt_dt AND wk_end_dt
         """.stripMargin
    })

    currentProdWeekDf
      .select("tm_pd_id")
      .head()
      .getInt(0)
    //.getString(0)
  }

  /* default for just previous week, pass (noOfPrevWeek*-7) if looking for nth previous week Production periodid
   * returns previous production weekid 20194926 if current weekid is 20195026
   * */
  def getPrevProdWeekPeriodId(daysAdd: Int = weekDays): Int = {
    getCurrentProdWeekPeriodId(daysAdd)
  }

  // no parameters required, returns Current production weekNo e.g. 201950 based on current timestamp
  def getCurrentProdWeekNo(daysAdd: Int = 0): Int = {
    val currentProdWeekDf = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Production week info by current date"
      override val sqlStatement: String =
        s"""
           |SELECT
           |  dspsnd_rx_prd_yr_wk_nbr
           |FROM ${timeWkDim.tableName}
           |WHERE UPPER(wk_strt_day_nm) = UPPER('$prodWeekStartDay')
           |  AND DATE_ADD(${currentTs.sparkSqlUTCText}, $daysAdd) BETWEEN wk_strt_dt AND wk_end_dt
         """.stripMargin
    })

    currentProdWeekDf
      .select("dspsnd_rx_prd_yr_wk_nbr")
      .head()
      .getInt(0)
    //.getString(0)
  }

  /* default for just previous week, pass (noOfPrevWeek*-7) if looking for nth previous week Production periodid
   * returns previous production weekNo 201949 if current weekid is 20195026
   * */
  def getPrevProdWeekNo(daysAdd: Int = weekDays): Int = {
    getCurrentProdWeekNo(daysAdd)
  }

  //pass production week no e.g. 201950 returns corresponding production weekInfo e.g (20195026,WEEK50)
  def getWeekInfo(weekNo: Int):DataFrame = {
    sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Current prod weekId info from week no"
      override val sqlStatement: String =
        s"""
           | SELECT
           |  *
           | FROM ${timeWkDim.tableName}
           | WHERE dspsnd_rx_prd_yr_wk_nbr = $weekNo
           |  AND wk_strt_day_nm = '$prodWeekStartDay'
         """.stripMargin
    })
  }

  //pass production weekId e.g. 20195026,include current week e.g. true/false, number of future weeks required e.g 6, returns List of weeks
  def getNextProdWeeks(weekNo: Int, includeCurrentWeek: Boolean = true, noOfNextWeek: Int = 1): DataFrame = {
    val NextProdWeeksDf = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Production Cycle"
      override val sqlStatement: String =
        s"""
           |WITH CTE AS (
           |  SELECT
           |    tm_pd_id as wk_tm_pd_id
           |    ,wk_strt_day_nm
           |    ,wk_strt_dt
           |    ,wk_end_dt
           |    ,wk_shrt_nm
           |    ,wk_nm
           |    ,dspnsd_rx_prd_wk_nbr
           |    ,dspsnd_rx_prd_yr_wk_nbr
           |    ,yr_strt_dt
           |    ,yr_end_dt
           |    ,yr_shrt_nm
           |    ,dspnsd_rx_prd_yr_nbr
           |    ,(ROW_NUMBER () over( order by tm_pd_id ASC ))-1 AS nextProdWeekOrder
           |  FROM ${timeWkDim.tableName}
           |  WHERE dspsnd_rx_prd_yr_wk_nbr >= $weekNo
           |  AND wk_strt_day_nm = '$prodWeekStartDay'
           |)
           |SELECT *
           |FROM CTE
           |WHERE nextProdWeekOrder <= $noOfNextWeek
          """.stripMargin
    })
    if (includeCurrentWeek == false) {
      NextProdWeeksDf
        .select("*")
        .filter("nextProdWeekOrder <> 0")
    }
    else
      NextProdWeeksDf
  }

  //this method is used to fetch open period cycle from mdata table
  def getOpenPeriod(periodLevelCode: String = "WEEKLY"): DataFrame = {
    sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Get Production Cycle Status"
      override val sqlStatement: String =
        s"""
           |SELECT *
           |FROM ${mDataPrdCyc.tableName}
           |WHERE UPPER(asset_cd) = UPPER('$asset_cd')
           |AND prd_cyc_end_ts is null
           |AND current_timestamp() between sys_eff_ts and sys_expry_ts
           |AND prd_cyc_typ_desc = '${periodLevelCode}'
          """.stripMargin
    })
  }

  //this method is to fetch last(max) closed period cycle from mdata table
  def getLastClosedPeriod(periodLevelCode: String ="WEEKLY"): DataFrame = {
    sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Get max closed Production Cycle "
      override val sqlStatement: String =
        s"""
           |SELECT MAX(pd_nbr)
           |FROM ${mDataPrdCyc.tableName}
           |WHERE UPPER(asset_cd) = UPPER('$asset_cd')
           |AND prd_cyc_end_ts is not null
           |AND current_timestamp() between sys_eff_ts and sys_expry_ts
           |AND prd_cyc_typ_desc = '${periodLevelCode}'
          """.stripMargin
    })
  }

  // no parameters required, returns Current production weekNo e.g. 201950 based on opened production cycle
  def getCurrentProdCycleNo(periodLevelCode: String = "WEEKLY"): Int = {
    getOpenPeriod(periodLevelCode).select(col("prd_cyc_id")).head.getString(0).replaceFirst("(.*)\\_(.*)\\_", "").toInt //YYYYWW26 for WEEKLY
  }

  def getCurrentProdPeriodNo(periodLevelCode: String = "WEEKLY"): Int = {
    Try(getOpenPeriod(periodLevelCode).select(col("pd_nbr")).head.getInt(0)).getOrElse(-1) // return YYYYWW for WEEKLY, return -1 when no opened prod cylce is available
  }


  def getPeriodStatus( periodId: Int= 0): Unit  = {

    //will be defined once signOff table is designed is completed
  }

  //pass production weekId e.g. 20195026,include current week e.g. true/false, number of previous weeks required e.g 6, returns List of weeks
  def getPrevProdWeeks(weekPeriodId: Int = getPrevProdWeekPeriodId(), includeCurrentWeek: Boolean = true, noOfPrevWeek: Int = 1): DataFrame ={

    val PrevProdWeeksDf = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Production weekly calendar"
      override val sqlStatement: String =
        s"""
           |WITH CTE AS(
           |  SELECT
           |    tm_pd_id as wk_tm_pd_id
           |    ,wk_strt_day_nm
           |    ,wk_strt_dt
           |    ,wk_end_dt
           |    ,wk_shrt_nm
           |    ,wk_nm
           |    ,dspnsd_rx_prd_wk_nbr
           |    ,dspsnd_rx_prd_yr_wk_nbr
           |    ,yr_strt_dt
           |    ,yr_end_dt
           |    ,yr_shrt_nm
           |    ,dspnsd_rx_prd_yr_nbr
           |    ,(ROW_NUMBER () over( order by tm_pd_id DESC ))-1 AS prevProdWeekOrder
           |  FROM ${timeWkDim.tableName}
           |  WHERE tm_pd_id <= $weekPeriodId
           |  AND wk_strt_day_nm = '$prodWeekStartDay'
           |)
           |SELECT *
           |FROM CTE
           |WHERE prevProdWeekOrder <= $noOfPrevWeek
         """.stripMargin
    })

    if (includeCurrentWeek == false){
      PrevProdWeeksDf
        .select("*")
        .filter("prevProdWeekOrder <> 0")
    }
    else
      PrevProdWeeksDf
  }

  //pass production weekId e.g. 20195026,include current week e.g. true/false, number of previous weeks required e.g 6, returns List of previous days with week(s)
  def getPrevProdDays(weekPeriodId: Int = getPrevProdWeekPeriodId(), includeCurrentWeek: Boolean = true, noOfPrevWeek: Int = 1): DataFrame ={

    val prevWeeksDf = getPrevProdWeeks(weekPeriodId, includeCurrentWeek, noOfPrevWeek)

    val tempPrevWeeksTable = s"prevWeeksDf"
    sparkExecutor.sql.registerAsTempTable(prevWeeksDf, s"Temptable", s"$tempPrevWeeksTable")

    sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Production daily calendar"
      override val sqlStatement: String =
        s"""
           |SELECT
           |   D.tm_pd_id as dy_tm_pd_id
           |  ,D.day_nm
           |  ,D.day_in_wk_sat_strt_nbr
           |  ,W.wk_tm_pd_id
           |  ,W.wk_strt_day_nm
           |  ,W.wk_strt_dt
           |  ,W.wk_end_dt
           |  ,W.wk_shrt_nm
           |  ,W.wk_nm
           |  ,W.dspnsd_rx_prd_wk_nbr
           |  ,W.dspsnd_rx_prd_yr_wk_nbr
           |  ,W.yr_strt_dt
           |  ,W.yr_end_dt
           |  ,W.yr_shrt_nm
           |  ,W.dspnsd_rx_prd_yr_nbr
           |  ,W.prevProdWeekOrder
           |FROM ${timeDyDim.tableName} D
           |INNER JOIN ${tempPrevWeeksTable} W
           |  ON D.wk_sat_strt_tm_pd_id = W.wk_tm_pd_id
         """.stripMargin
    })
  }
  //pass production Month no e.g. 202005 returns corresponding production MontInfo e.g (2020053,May)
  def getMonthInfo(MonthNo: Int):DataFrame = {
    sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Current prod MonthId info from Month no"
      override val sqlStatement: String =
        s"""
           | SELECT
           |  *
           | FROM ${timeMthDim.tableName}
           | WHERE cast(concat(cast(yr_nbr as string), lpad(cast(mth_nbr as string), 2, "0")) as int) = $MonthNo
         """.stripMargin
    })
  }

  //pass production weekId e.g. 20195026,include current week e.g. true/false, number of future weeks required e.g 6, returns List of weeks
  def getNextProdMonths(MonthNo: Int, includeCurrentMonth: Boolean = true, noOfNextMonth: Int = 1): DataFrame = {
    val NextProdMonthsDf = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = "Production Cycle"
      override val sqlStatement: String =
        s"""
           |WITH CTE AS (
           |  SELECT
           |tm_pd_id
           |,cast(concat(cast(yr_nbr as string), lpad(cast(mth_nbr as string), 2, "0")) as int) AS yr_mth_nbr
           |,mth_strt_dt
           |,mth_end_dt
           |,mth_shrt_nm
           |,mth_nm
           |,mth_nbr
           |,qtr_tm_pd_id
           |,qtr_strt_dt
           |,qtr_end_dt
           |,qtr_shrt_nm
           |,qtr_nm
           |,qtr_nbr
           |,mth_in_qtr_nbr
           |,half_yr_tm_pd_id
           |,half_yr_strt_dt
           |,half_yr_end_dt
           |,half_yr_shrt_nm
           |,half_yr_nm
           |,half_yr_nbr
           |,mth_in_half_yr_nbr
           |,yr_tm_pd_id
           |,yr_strt_dt
           |,yr_end_dt
           |,yr_shrt_nm
           |,yr_nm
           |,yr_nbr
           |,mth_in_yr_nbr
           |,publ_eff_ts
           |,bus_eff_dt
           |,publ_expry_ts
           |,bus_expry_dt
           |,(ROW_NUMBER () over( order by tm_pd_id ASC ))-1 AS nextProdMonthOrder
           |  FROM ${timeMthDim.tableName}
           |  WHERE cast(concat(cast(yr_nbr as string), lpad(cast(mth_nbr as string), 2, "0")) as int) >= $MonthNo
           |)
           |SELECT *
           |FROM CTE
           |WHERE nextProdMonthOrder <= $noOfNextMonth
          """.stripMargin
    })
    if (includeCurrentMonth == false) {
      NextProdMonthsDf
        .select("*")
        .filter("nextProdMonthOrder <> 0")
    }
    else
      NextProdMonthsDf
  }
}
