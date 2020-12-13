package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{BemConf, KafkaOffsetPersistance}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service.{AbstractServiceTrait, SparkServiceTrait}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical.OdsLoadedEvent
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical.OdsLoadedEventProtocol._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.dwh.DwhLoadProcess
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.dwh.dwhProcessor._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.EmailAlerts
import org.joda.time.DateTime
import spray.json._


/**
  * The input arguments to trigger the spark function
  *
  * (none exists at present)
  */
case class ProcessDwhStreamArgs(startOffset: Long = -1)

object ProcessDwhStreamConstants {
  val SRC_SYS_CD_ODS: Short = 1
  val SRC_SYS_CD_DI: Short =  2
  val SRC_SYS_CD_DP: Short =  3
  val SRC_SYS_CD_BDC: Short = 1
  val SRC_SYS_CD_DATA_INTEGRITY: String = "Data Integrity"
  val SRC_SYS_CD_ODS_PROC_SOURCE: String = "SOURCE"
  val SRC_SYS_CD_RECOVERY_PROC_SOURCE = "RECOVERY"
  val SRC_SYS_CD_REBRIDGING_PROC_SOURCE = "REBRIDGING"
  val SRC_SYS_CD_SUMMARY_PROC_SOURCE = "SUMMARY"
  val SRC_SYS_CD_BDC_PROD_PROC_SOURCE: String = "BDC PCMDTY"
  val SRC_SYS_CD_BDC_RI_PROC_SOURCE: String = "PBS DWH BDC RI"
  val SRC_SYS_CD_BDC_LOC_PROC_SOURCE: String = "LocationCoding"
  val SRC_SYS_CD_BDC_ARTF_PROC_SOURCE: String = "ArtificialData"
  val SRC_SYS_CD_ODS_PROC_HISTORY: String = "history"
  val SRC_SYS_CD_ODS_PROC_RESUPLY: String = "resupply"
  //val SRC_SYS_CD_DI_PROC_SOURCE = "DataImputation"
  val SRC_SYS_CD_DI_PROC_SOURCE = "IMPTN"
  val DATA_TYPE_DI_PUBLIC = "public"
  val DATA_TYPE_DI_TEST = "test"
  val DELETE_IND_CURRENT: String = "0"
  val DELETE_IND_DELETED: String = "1"
  val PROCESS_NAME = "ProcessDwhStream"
  val SUMM_PROCESS_NAME = "SUMMARYBUILD"
//  val DATA_TYPE_PUBLIC = "PUBLIC"
  val BDC_DATA_TYPE_APPROVED = "APPROVED"
  val BDC_DATA_TYPE_REJECTED = "REJECTED"
  val BDC_DATA_TYPE_TEST = "TEST"
  val BDC_DIR_NAME_APPROVED = BDC_DATA_TYPE_APPROVED.toLowerCase()
  val BDC_DIR_NAME_TEST = BDC_DATA_TYPE_TEST.toLowerCase()
  val SRC_SYS_CD_FSL_PROC = "FSL"
  val OAC_RLC_MAP_PROC = "PharmacyRelocMap"
  val DAILY_PERIOD_LEVEL_CODE: String = "DAILY"
  val WEEKLY_PERIOD_LEVEL_CODE: String = "WEEKLY"
  val MONTHLY_PERIOD_LEVEL_CODE: String = "MONTHLY"
  val QUARTERLY_PERIOD_LEVEL_CODE: String = "QUARTERLY"
  val HALFYEARLY_PERIOD_LEVEL_CODE: String = "HALFYEARLY"
  val DAILY_PERIOD_LEVEL_ID: String = "1"
  val WEEKLY_PERIOD_LEVEL_ID: String = "2"
  val MONTHLY_PERIOD_LEVEL_ID: String = "3"
  val QUARTERLY_PERIOD_LEVEL_ID: String = "4"
  val HALFYEARLY_PERIOD_LEVEL_ID: String = "5"
  val startIndex: Int = 1
  val endIndex : Int = 6

  val SRC_SYS_CD_LMS_PROC = "PRODUCT REFERENCE LOAD"

  val SRC_SYS_CD_DAQE = "DAQE"

}

object ProcessNameConstants {

  val DWH_PROC_NM = "NDH DWH DSUPP"
  val RECOVERY_PROC_NM = "NDH DWH RCVRY"
  val REBRIDGING_PROC_NM = "NDH REBRDG"
  val SUMMARY_PROC_NM = "NDH DWH SUM"
  val BDC_PROD_PROC_NM = "NDH DWH BDC PCMDTY"
  val DI_PROC_NM = "NDH DWH IMPTN"
  val BDC_RI_PROC_NM = "NDH DWH BDC RI"
  val FSL_PROC_NM_SOURCE = "NDH DWH FSL"
  val OAC_RLC_MAP_PROC_NM = "NDH DWH OACRLC"
  val DT2_IMPTN_PROC_NM_SOURCE = "NDH DT2 IMPTN"
  val DT2_PROJTN_PROC_NM_SOURCE = "NDH DT2 PROJTN"

}

object DwhSummaryBuildConstants {

  val FSL_STATUS_PROJECTED = "PROJECTED"
  val FSL_OPRTNL_STAT_CD = 0
  val DAILY_WEEKLY_FREQ = "DAILY"
  val CORRELATION_UUID = java.util.UUID.randomUUID().toString.toUpperCase()
  //val PROC_NM = "DWH_SUMMARY_BUILD"
  val SUM_SNAPSHOT_VERS_NM_PRIOR = "prior"
  val SUM_SNAPSHOT_VERS_NM_CURRENT = "current"
  val RE_PARTITION = 12
  val SUMM_SUBJ_AREA_NM = "PRESCRIPTIONSERVICE"
  val SUMM_SUB_SUBJ_AREA_NM_WK = "SUMMARYWEEKLY"
  val PRD_CYC_WK_TM_PD_NBR = "dervd_prd_cyc_yr_wk_nbr"
  val WEEKLY_PERIOD_LEVEL_CODE = "WEEKLY"
  val OAC_RELOC_MAP_OPRTNL_STAT_CD = 0
}

case class DwhEmailStatsInitialization(
                                        var dwhInputDeltaCount: Long = 0
                                        , var dwhSupplierName: String = ""
                                        , var dwhProcessName: String = ""
                                        , var dwhKafkaTopic: String = ""
                                        , var dwhKafkaOffset: Long = 0
                                        , var dwhActiveRecCount: Long = 0
                                        , var dwhDeactivedRecCount: Long = 0
                                        , var dwhStartTime: DateTime = new DateTime
                                      )

object DwhEmailStats extends DwhEmailStatsInitialization

/**
  * Created by DJha on 26/03/2018.
  */
object ProcessDwhStream extends ProcessDwhStreamTrait {
}

trait ProcessDwhStreamTrait extends AbstractServiceTrait with SparkServiceTrait {

  var correlationUuid: Option[String] = None

  val kafkaTable = new MDataKafkaOffset(appContext)
  val kafkaOffsetPersistance = new KafkaOffsetPersistance(appContext, sparkExecutor.sql, kafkaTable)
  val emailAlerts = new EmailAlerts(appContext, sparkExecutor)

  def readEvent(i: Long): (OdsLoadedEvent, String, Int, Long) = {
    println(s"Checking for events (DWH)")

    val kafkaTopicName = appContext.config.getString("KAFKA_TOPIC_DWH_INPUT").trim
    val kafkaMessage = sparkExecutor.kafka.readSingleKafkaEvent(
      kafkaTopicName,
      if (i < 0) kafkaOffsetPersistance.readOffsetData(kafkaTopicName, ProcessDwhStreamConstants.PROCESS_NAME)
      else i // override value provided as parameter
    )

    val eventCount = kafkaMessage.count()
    println(s"Received ${eventCount} events")

    if (eventCount > 1) {
      val e = new Exception(s"Too many message read from kafka topic ${kafkaTopicName}: ${eventCount} - expected 0 or 1!")
      //appContext.logger.logError(e)
      throw e
    }

    if (eventCount == 1)
    {
      val (event, topic, partition, offset) = kafkaMessage.head()
      DwhEmailStats.dwhKafkaOffset = offset
      DwhEmailStats.dwhKafkaTopic = topic

      try {
        println(
          s"""
             |Received:
             |  event: $event
             |  topic: $topic
             |  partition: $partition
             |  offset: $offset
         """.stripMargin)
        (event.parseJson.convertTo[OdsLoadedEvent](jsonReader), topic, partition, offset)
      }
      catch {
        case e: Exception =>
          println("Incorrect message format " + event)
          //appContext.logger.logError(e)
          throw e
      }
    }
    else
      return (null, null, 0, 0)
  }

  def actionOnEventFailed(e: Exception, topic: String = "", partition: Int = 0, offset: Long = 0 ,event: OdsLoadedEvent): Unit = {


    if(e.getLocalizedMessage().startsWith(s"Warning- ${event.src_proc_pit_id} Process Load Status:")){
     /* appContext.bemLogger.logBusinessEventFinish(
        eventDescription = s"Operation finished with warning message: ${e.getMessage}",
        eventOutcome = BemConf.EventOutcome.WARNING
      )*/
      emailAlerts.sendWarningMail(e.getLocalizedMessage().replace("offsetvalue",{offset.toString}), s"Process Load Status:${event.src_proc_pit_id} records were found to be Zero")
      kafkaOffsetPersistance.saveOffsetData(topic, partition, ProcessDwhStreamConstants.PROCESS_NAME, offset)
    }
    else {
      /*appContext.bemLogger.logBusinessEventFinish(
        eventDescription = s"Operation failed with following error: ${e.getMessage}",
        eventOutcome = BemConf.EventOutcome.FAILURE
      )*/
      emailAlerts.sendDwhFailureStatsEmail(e, correlationUuid getOrElse "--")
      throw e
    }
  }

  private def replaceNull(input: String): String = {
    if (input == null) return ""
    input
  }

  def processEvents(kafkaMessageData: (OdsLoadedEvent, String, Int, Long)): Unit = {

    val preProcessors = List(
       new DwhOdsProcessor(appContext, sparkExecutor)
    )

    val (event, topic, partition, offset) = kafkaMessageData
    println(s"Received new event: $event")
    correlationUuid = Some(event.impl_btch_pit_id)
    DwhEmailStats.dwhProcessName = event.src_proc_pit_id
    DwhEmailStats.dwhSupplierName = event.supld_dsupp_proc_id

    try {

      // Pipeline
      var bemLoggerEventScope:String = ""
      var bemLoggerDataName:String = ""
      if (event.src_proc_pit_id.toUpperCase() == ProcessDwhStreamConstants.SUMM_PROCESS_NAME ||
          event.src_proc_pit_id.toUpperCase() == ProcessDwhStreamConstants.SRC_SYS_CD_RECOVERY_PROC_SOURCE ||
          event.src_proc_pit_id.toUpperCase() == ProcessDwhStreamConstants.SRC_SYS_CD_REBRIDGING_PROC_SOURCE
      )
      {
        bemLoggerEventScope = event.src_proc_pit_id.toUpperCase()
        bemLoggerDataName = bemLoggerEventScope
      } else {
        bemLoggerEventScope = event.supld_dsupp_proc_id
        bemLoggerDataName = event.data_typ_shrt_nm
      }

      var batchUUID  = event.impl_btch_pit_id

      //Setting Bem Logger context for DWH Process
     /* appContext.bemLogger.setBemLoggerContext(
        correlationUUID = batchUUID,
        dataName = bemLoggerDataName,
        eventScope = bemLoggerEventScope,
        productionCycle = event.dervd_dsupp_file_tm_pd_nbr.toString
      )*/

      DwhLoadProcess(
         appContext,
         preProcessors,
         kafkaOffsetPersistance,
         emailAlerts
      ).execute(event, topic, partition, offset)


    } catch {
      case e: Exception =>
        actionOnEventFailed(e, topic, partition, offset,event)
    }
  }

  def main(args: Array[String]): Unit = {

    val opts = ProcessDwhStreamArgs()
    val argParser = new scopt.OptionParser[ProcessDwhStreamArgs]("pbs") {
      head(s"gb9-pbs", s"${getClass().getPackage().getImplementationVersion()}-${getClass().getPackage().getSpecificationVersion()}")
      help("help").text("Prints this usage text.")

      opt[String]('o', "startOffset")
        .action((x, c) => c.copy(startOffset = x.toLong))
        .text("Starting offset (overrides preserved one)")
    }
    argParser.parse(args, opts) match {
      case Some(opts) => {
        var kafkaMessageData = readEvent(opts.startOffset)
        while (kafkaMessageData._1 != null) {
          processEvents(kafkaMessageData)
          val newOffset = kafkaMessageData._4 + 1
          kafkaMessageData = readEvent(newOffset)
        }
      }
      case None => {
        throw new IllegalArgumentException("Incorrect or no input arguments passed. Try --help to get usage text.")
      }
    }
  }
}
