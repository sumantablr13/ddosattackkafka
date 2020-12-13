package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh.ProcessDwhStreamConstants
import org.apache.spark.sql.types._

/**
  * Created by Shiddesha.Mydur on 3rd Jul 2019
  *
  */
abstract class AbstractMDataPrdCyc(appContext: AppContext) extends PartitionTable("") {

  override lazy val logicalName: String = "Metadata Production Cycle"
  override lazy val physicalName: String = "mdata_prd_cyc"
  val startIndex = ProcessDwhStreamConstants.startIndex
  val endIndex = ProcessDwhStreamConstants.endIndex

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("prd_cyc_id", "STRING"),
    ("prd_cyc_typ_cd", "STRING"),
    ("prd_cyc_typ_desc", "STRING"),
    ("pd_nbr", "INT"),
    ("prd_cyc_strt_ts", "TIMESTAMP"),
    ("prd_cyc_end_ts", "TIMESTAMP"),
    ("latest_rec_ind", "INT"),
    ("isrted_by_usr_nm", "STRING"),
    ("sys_eff_ts", "TIMESTAMP"),
    ("sys_expry_ts", "TIMESTAMP")
  )

  override lazy val partitionColumns: Array[(String, String)] = Array(
    ("asset_cd", "STRING")
  )

  override lazy val schema: StructType = (new StructType)
    .add("prd_cyc_id", StringType)
    .add("prd_cyc_typ_cd", StringType)
    .add("prd_cyc_typ_desc", StringType)
    .add("pd_nbr", IntegerType)
    .add("prd_cyc_strt_ts", TimestampType)
    .add("prd_cyc_end_ts", TimestampType)
    .add("latest_rec_ind", IntegerType)
    .add("isrted_by_usr_nm", StringType)
    .add("sys_eff_ts", TimestampType)
    .add("sys_expry_ts", TimestampType)
    .add("asset_cd", StringType)

  override lazy val createTableOpts: String =
    """
      | ROW FORMAT DELIMITED
      | FIELDS TERMINATED BY '\001'
      | LINES TERMINATED BY '\n'
      | STORED AS TEXTFILE
    """.stripMargin

  def helperCreatePartitionSpec(assetCd: String): String = s"asset_cd='$assetCd'"

  def helperInsertStatement(
                             assetCode: String,
                             periodLevelCode: String,
                             periodId: Int,
                             sys_eff_ts : TimestampConverter
                           ): String = {

    val shortPeriodLevelCode = periodLevelCode match {
      case "DAILY" => "DAY"
      case "WEEKLY" => "WK"
      case "MONTHLY" => "MTH"
      case _ => "OTHER"
    }

    s"""
       SELECT
         '${assetCode}_${shortPeriodLevelCode}_${periodId}' as prd_cyc_id,
         '$shortPeriodLevelCode'                                as prd_cyc_typ_cd,
         '$periodLevelCode'                                     as prd_cyc_typ_desc,
          NULL                                                  as pd_nbr,
         ${sys_eff_ts.sparkSqlUTCText}                          as prd_cyc_strt_ts,
          NULL                                                  as prd_cyc_end_ts,
          0                                                     as latest_rec_ind,
         ''                                as isrted_by_usr_nm,
         ${sys_eff_ts.sparkSqlUTCText}                          as sys_eff_ts,
         ${TimestampConverter.TIMESTAMP_PLUS_INFINITY.sparkSqlUTCText} as sys_expry_ts,
        '$assetCode'                                            as asset_cd
     """
  }
}
