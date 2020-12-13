package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.ods

import java.io.FileNotFoundException

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service.SparkServiceTrait
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{BemConf, KafkaOffsetPersistance}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.exception.{FileSizeCountException, ZeroDRecordsFoundException}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.DAQELoadedEvent
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.SrcKafkaEventProtocol._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.ods.MappingProcess
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.supplierPreProcessor.{FileFormatSSTTbl1PreProcessor, FileFormatSSTTbl2PreProcessor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.{EmailAlerts, FileContentValidation, ValidationRunner}
import kafka.message.InvalidMessageException
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.Dataset
import org.joda.time.DateTime
import spray.json._

/**
  * The input arguments to trigger the spark function
  *
  * @param startOffset - read kafka offsets
  *
  */
case class ProcessDcpStreamArgs(
                                 startOffset: Long = -1,
                                 kafkaTopicName: String = ""
                               )

object ProcessDcpStreamConstants {
  /*val BRDG_STATUS_BRIDGED = "BRIDGED"
  val BRDG_STATUS_IGNORED = "IGNORED"
  val BRDG_STATUS_UNKNOWN = "UNKNOWN"
  val BRDG_STATUS_UNBRIDGED = "UNBRIDGED"
  val BRDG_METHOD_AUTO = "AUTO"
  val BRDG_METHOD_MANUAL= "MANUAL"
  val BRDG_REF_IMPL_ID_KFBT = "DK9 KF BT"
  val BRDG_REF_IMPL_ID_MBT = "DK9 BDF MBT"
  val BRDG_REF_IMPL_ID_RDS = "DK9 BDF RDS"
  val PROCESS_NAME = "ProcessDCPStream"*/
  val ODS_PRCC_NM = "NDH ODS DSUPP"
}

case class OdsEmailStatsInitialization(var fileHdrCnt: Long = 0
                                       , var fileDtlCnt: Long = 0
                                       , var fileTrlrCnt: Long = 0
                                       , var tblHdrCnt: Long = 0
                                       , var tblDtlCnt: Long = 0
                                       , var tblTrlrCnt: Long = 0
                                       , var odsFactTblCnt: Long = 0
                                       , var ddupCnt: Long = 0
                                       , var totDspnsdUnits: BigDecimal = 0.0
                                       , var totPscbrUnits: BigDecimal = 0.0
                                       , var odsStartTime: DateTime = new DateTime()
                                       , var odsEndTime: DateTime = new DateTime()
                                       , var kafkaOffset: Long = 0)

object OdsEmailStats extends OdsEmailStatsInitialization

trait ProcessDcpStreamTrait extends SparkServiceTrait {

  val kafkaTable = new MDataKafkaOffset(appContext)
  val kafkaOffsetPersistance = new KafkaOffsetPersistance(appContext, sparkExecutor.sql, kafkaTable)
  val emailAlerts = new EmailAlerts(appContext, sparkExecutor)
  val smtpHost = appContext.config.getString("email.smtpHost").trim.toLowerCase()

  val defaultFileContentValidations = Seq(
   // FileContentValidation.NumericDataTypeValidation(appContext),
    //    FileContentValidation.BlankTransDateValidation(appContext)
  //  FileContentValidation.EmptyValueValidation(appContext),
    //FileContentValidation.DecimalDataTypeValidation(appContext),
    //FileContentValidation.DecimalDataTypeWithCommaValidation(appContext),
 //   FileContentValidation.ZeroValueValidation(appContext),
    //FileContentValidation.QuantityNumericDataTypeValidation(appContext),
 //   FileContentValidation.NegativeValueValidation(appContext)
    //    FileContentValidation.YearNbrValidation(appContext),
    //    FileContentValidation.MonthNbrValidation(appContext),
    //    FileContentValidation.DayNbrValidation(appContext)
    //FileContentValidation.BlankClientCodeValidation(appContext),
    //FileContentValidation.QuantityDecimalDataTypeValidation(appContext)
  )

  val defaultHeaderValidations = Seq(
    //    HeaderValidation.DayNbrValidation(appContext),
    //    HeaderValidation.WeekNbrValidation(appContext),
    //    HeaderValidation.MonthNbrValidation(appContext),
    //    HeaderValidation.YearNbrValidation(appContext),
    //HeaderValidation.NumericDataTypeValidation(appContext)
  )


  // val defaultValidationsRunner = new ValidationRunner(defaultFileContentValidations, defaultHeaderValidations)

  val defaultValidationsRunner = new ValidationRunner()

  def getkafkaTopicName: Seq[String]

  def main(args: Array[String]): Unit = {

    //appContext.logger.module(this.getClass.getName)

    val opts = ProcessDcpStreamArgs()
    val argParser = new scopt.OptionParser[ProcessDcpStreamArgs]("ndho") {
      head(s"dk9-ndho", s"${getClass.getPackage.getImplementationVersion}-${getClass.getPackage.getSpecificationVersion}")
      help("help").text("Prints this usage text.")

      opt[String]('o', "startOffset")
        .action((x, c) => c.copy(startOffset = x.toLong))
        .text("Starting offset (overrides preserved one)").optional()

      opt[String]('t',"kafkaTopicName")
        .action((x,c)=>c.copy(kafkaTopicName=x))
        .text("Assigning Kafka TopicName")
        .optional()
        .validate(x =>
          if( getkafkaTopicName.contains(x)) success
          else failure("Invalid Kafka Topic Name supplied")
        )
    }
    argParser.parse(args, opts) match {
      case Some(opts) =>
        val kafkaTopicList = if (opts.kafkaTopicName.isEmpty) getkafkaTopicName else Seq(opts.kafkaTopicName)
        kafkaTopicList.foreach( kafkaTopicName =>{
          var kafkaMessage = readEvent(opts.startOffset, kafkaTopicName)
          var kafkaEvent: DAQELoadedEvent = kafkaMessage._1
          while (kafkaEvent != null) {
            val eventOfSupplier = addEventsToAccumulator(kafkaMessage)
            if (kafkaEvent.status.equalsIgnoreCase("Success")) {
              OdsEmailStats.kafkaOffset = kafkaMessage._4
              processAccumulator(eventOfSupplier)
            }
            else {
             // appContext.logger.logMessage(s"Skipped kafka having status failure. Kafka Topic : ${eventOfSupplier.topic}  Kafka Offset : ${eventOfSupplier.offset} batchTable: ${eventOfSupplier.batchTable} ")
              kafkaOffsetPersistance.saveOffsetData(eventOfSupplier.topic, eventOfSupplier.partition, "ods", eventOfSupplier.offset)
            }

            val newOffset = kafkaMessage._4 + 1
            kafkaMessage = readEvent(newOffset, kafkaTopicName)
            kafkaEvent = kafkaMessage._1
          }
        })
      case None =>
        throw new IllegalArgumentException("Incorrect or no input arguments passed. Try --help to get usage text.")


    }
  }

  def readEvent(offset: Long, kafkaTopicName: String): (DAQELoadedEvent, String, Int, Long) = {
    val kafkaCustomServer = appContext.config.getString("KAFKA_SERVER_DAQE")
    val kafkaMessage: Dataset[(String, String, Int, Long)] =
      sparkExecutor.kafka.readSingleKafkaEvent(
        kafkaTopicName,
        if (offset < 0) kafkaOffsetPersistance.readOffsetData(kafkaTopicName, "ods") else offset,
        kafkaCustomServer
      )
    kafkaMessage.show()
    val kafkaEventCount = kafkaMessage.count()
    println("kafka event count is " + kafkaEventCount)
    if (kafkaEventCount == 0) {
      (null, null, 0, 0)
    }
    else {
      try {
        val srcSuppEvents = kafkaMessage.first() match {
          case (event, topic, partition, offset) =>
            try {
              println(s"Event is event: $event, topic: $topic, partition: $partition, offset: $offset")
              (event.replaceAll("\\s", "")
                .replaceAll("\\{\\}", "")
                .parseJson.convertTo[DAQELoadedEvent](jsonReader), topic, partition, offset)
            }
            catch {
              case e: Exception =>
                println("Incorrect message format " + event)
                Option.empty[(DAQELoadedEvent, String, Int, Long)].orNull
            }
        }
        srcSuppEvents
      } catch {
        case nse: NoSuchElementException => {
         // appContext.logger.logError(nse)
         // appContext.logger.logMessage("Kafka is empty.")
          Option.empty[(DAQELoadedEvent, String, Int, Long)].orNull
        }
        case npe: NullPointerException => {
         // appContext.logger.logError(npe)
         // appContext.logger.logMessage("Kafka is empty.")
          Option.empty[(DAQELoadedEvent, String, Int, Long)].orNull
        }
        case exe: Exception => {
          //appContext.logger.logError(exe)
          Option.empty[(DAQELoadedEvent, String, Int, Long)].orNull
        }
      }
    }
  }

  def addEventsToAccumulator(dcpSuppEvents: (DAQELoadedEvent, String, Int, Long)): SupplierRecordsFile = {
    val supplierFileDetails = dcpSuppEvents match {
      case (event, topic, partition, offset) =>
        val action = event.action
        val batchTable = event.payload.batchTable
        val eventType = event.eventType
        val status = event.status
        val ts = event.ts
        val tabToValidate = event.payload.tabToValidate
        val schema = event.payload.targetSchema
        val runId = event.payload.runId
        val layoutName = event.payload.layoutName

        eventType match {
          case "QC0Validations" =>
            SupplierRecordsFile(
              topic,
              partition,
              offset,
              action,
              eventType,
              ts,
              batchTable,
              schema,
              tabToValidate,
              runId,
              layoutName
            )
          case _ =>
            println("**********************Stop The Process******************************")
            throw new InvalidMessageException("Invalid event" + eventType)
        }
    }
    supplierFileDetails
  }

  def processAccumulator(eventOfSupplier: SupplierRecordsFile): Unit = {
    val preProcessors = Map[String, SupplierPreProcessor](
      "dk9_sst_tbl_2_load" -> FileFormatSSTTbl2PreProcessor(appContext, sparkExecutor, defaultValidationsRunner),
      "dk9_sst_tbl_1_load" -> FileFormatSSTTbl1PreProcessor(appContext, sparkExecutor, defaultValidationsRunner)
    )

    val event = eventOfSupplier
    try {

      //Setting Bem Logger context
      /*appContext.bemLogger.setBemLoggerContext(
        correlationUUID = event.runId,
        dataName = event.layoutNm,
        eventScope = event.batchTable,
        productionCycle = event.action
      )*/
      /*appContext.bemLogger.logBusinessEventStart(
        eventDescription = "Source File existence validation has been started",
        eventOperation = BemConf.EventOperation.ODS_SRC_FILE_EXISTENCE_VALIDATION,
        componentIdentifier = "ODS Supplier PreProcessor"
      )*/
      OdsEmailStats.odsStartTime = new DateTime()

      MappingProcess(
        preProcessors
        , appContext
        , sparkExecutor
        , appContext.config.getString("KAFKA_TOPIC_DWH_INPUT")
        , appContext.config.getInt("ODS_ERROR_THRESHOLD_VALUE")
      ).execute(event)

      kafkaOffsetPersistance.saveOffsetData(event.topic, event.partition, "ods", event.offset)
    } catch {
      case e: NoSuchElementException => {
        if (e.getLocalizedMessage.startsWith("No preprocessor found")) {
          emailAlerts.sendOdsFailureStatsEmail(event, e)
          kafkaOffsetPersistance.saveOffsetData(event.topic, event.partition, "ods", event.offset)
          actionOnEventFailed(e)
        }
      }
      case e: FileNotFoundException => {
        if (e.getLocalizedMessage.startsWith("Supplier file doesn't exists...!")) {
          emailAlerts.sendOdsFailureStatsEmail(event, e)
          kafkaOffsetPersistance.saveOffsetData(event.topic, event.partition, "ods", event.offset)
          actionOnEventFailed(e)
        }
      }
      case e: FileSizeCountException => {
        if (e.getLocalizedMessage.startsWith("Found File size is zero!")) {
          emailAlerts.sendOdsFailureStatsEmail(event, e)
          kafkaOffsetPersistance.saveOffsetData(event.topic, event.partition, "ods", event.offset)
          actionOnEventFailed(e)
        }
      }
      case e: Exception => {
        if (e.getLocalizedMessage.contains(s"Couldn't connect to host, port: ${smtpHost}")
          || e.getMessage.contains(s"Couldn't connect to host, port: ${smtpHost}")
          || ExceptionUtils.getStackTrace(e).contains(s"Couldn't connect to host, port: ${smtpHost}")) {
          kafkaOffsetPersistance.saveOffsetData(event.topic, event.partition, "ods", event.offset)
          emailAlerts.sendOdsFailureStatsEmail(event, e)
          actionOnEventFailed(e)
        } else {
          emailAlerts.sendOdsFailureStatsEmail(event, e)
          //kafkaOffsetPersistance.saveOffsetData(event.topic, event.partition, "ods", event.offset)
          actionOnEventFailed(e)
        }
      }
    }
  }

  def actionOnEventFailed(e: Exception): Unit = {
    /*appContext.logger.logError(e)
    appContext.logger.logMessage("APP_STATUS_INDICATOR", "FAILURE")
    appContext.logger.finishPipeline(eventOutcomeVal = appContext.logger.statusFailure, eventDescriptionVal = s"Step finished with error ${e.getMessage}")
    appContext.bemLogger.logBusinessEventFinish(
      eventDescription =
        s"Operation failed with following error: ${e.getMessage}",
      eventOutcome = BemConf.EventOutcome.FAILURE
    )*/
  }
}
