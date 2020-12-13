package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.ods

import java.sql.Timestamp

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.SparkExecutor
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, BemConf, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.{DataFormatValidationTrait, EmailAlerts, Validation, ValidationRunner}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.{SupplierPreProcessor, SupplierRecordsFile}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh.ProcessDwhStreamConstants
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.ods.OdsEmailStats
import org.apache.spark.sql.functions.{col, lit, substring, unix_timestamp}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.DataFormatValidation.T_NdhOds

/**
  *
  * Created by DJha on 26/03/2018.
  * Updated by SMydur on 12/07/2018 for odsToDwhKafka Integration
  * Updated by Ashokkumar.C on 11/07/2019 - ODS to populate src_sys_btch_id instead of bcth_id while generating Kafka message for DWH.
  */
case class MappingProcess (
                           supplierProcessors: Map[String, SupplierPreProcessor]
                           , appContext: AppContext
                           , sparkExecutor: SparkExecutor
                           , odsReadyTopic: String
                           , odsErrRecThresholdValue: Int
                         ) {

  val validation = new ValidationRunner()

  def execute(event: SupplierRecordsFile) = {

    val eventDescription = "DAQE ready event"

    var (preProcessed, odsErrRecs, isFileDuplicated ) = prepareDataset(event)
    // odsErrRecs.show(2, false)

    preProcessed.cache()

    val triggerEmailAlert = appContext.config.getString("TRIGGER_EMAIL_ALERT").trim.toBoolean
    val emailAlerts = new EmailAlerts(appContext, sparkExecutor)

   // val bridged = processBridging (preProcessed, event, eventDescription).repartition(20).cache()
    val bridged = preProcessed
    processRules(bridged, event, eventDescription)
    loadOdsToDwhKafkaMessage(bridged, event, eventDescription)

  /*  appContext.bemLogger.logBusinessEventFinish(
      eventDescription =
        s"Writing into final table has been completed successfully for file ${event.layoutNm} with correlationUUID=${event.runId}. " +
          s"Record count: ${OdsEmailStats.odsFactTblCnt.toString}",
      eventOutcome = BemConf.EventOutcome.SUCCESS
    )  // ODS_WRITE*/
    if(triggerEmailAlert) emailAlerts.sendOdsSuccessStatsEmail(event, bridged.toDF(), odsErrRecs)

  }

  private def prepareDataset(event: SupplierRecordsFile): ( Dataset[T_NdhOds], DataFrame, Int) = {
    val fileFormatVersion = event.tabToValidate
    val preProcessor = supplierProcessors.get(fileFormatVersion) match {
      case Some(supplierPreProcessor) =>
        println(s"Keys found for the processor $fileFormatVersion")
       // appContext.logger.debug(s"Keys found for the processor $fileFormatVersion")
        supplierPreProcessor
      case None =>
       // appContext.logger.debug(s"No preprocessor found for fileFormatVersion: $fileFormatVersion !!")
        throw new NoSuchElementException(s"No preprocessor found for fileFormatVersion: $fileFormatVersion")
    }
   // appContext.logger.debug(s"Found a pre processor for fileFormatVersion ${event.tabToValidate}")
    preProcessor.preprocess(event,odsErrRecThresholdValue)

  }

  /*private def loadSupplierDuplicationLogs(event: SupplierRecordsFile, appContext: AppContext, sparkExecutor: SparkExecutor, dupErrStatus: Int, df: DataFrame) = {
    val validationDf = validation.populateSupplierValidationLog(event, appContext, sparkExecutor, dupErrStatus, df.toDF())
    val table = new PbsSupplierFileLoadValidation(appContext)
    sparkExecutor.sql.insertTable(table, validationDf, "Loading ODS File Content Validation/s Error Records into error log table",
      appContext.envApplicationCode, event.layoutNm, new TimestampConverter())
    //  val creDayNbr = df.limit(1).select("cre_day_nbr").head.getInt(0)
    //  validation.dashBoardMetadata(appContext,sparkExecutor,event.path,validationDf,table,creDayNbr.toString)
    sparkExecutor.sql.refreshTable(table) // refreshing metadata
  }

  private def processBridging(gdmRecords: Dataset[NdhOds], event: SupplierRecordsFile, eventDescription: String): Dataset[NdhOds] = {

    val bridging = Bridging(
      s"${appContext.envTenantCode.toUpperCase}_${appContext.envApplicationCode.toUpperCase}_${this.getClass.getSimpleName.toLowerCase.replaceAll("\\$", "")}"
      , 2
      , appContext
      , sparkExecutor
      , event
    )

    bridging.startBridgingStage()

    val productBridgingOut = bridging.productBridging(gdmRecords.toDF())

    val locationBridgingOut = bridging.locationBridging(sparkExecutor.file.encoderTest[NdhOds](productBridgingOut))

    //val depotBridgingOut = bridging.depotBridging(locationBridgingOut)

    //println("Bridging output")

    //depotBridgingOut.show(10)

    bridging.finishBridgingStage()

    locationBridgingOut
  }*/

  private def processRules(gdmRecords: Dataset[T_NdhOds], event: SupplierRecordsFile, eventDescription: String) = {
    val processor = Processor(
      s"${"SB"}_${"BS"}_${
        this.getClass.getSimpleName.toLowerCase
          .replaceAll("\\$", "")
      }"
      , appContext
      , sparkExecutor
    )

    //  processor.startPostProcessingStage()

    processor.loadOdsTable(gdmRecords, event)

    //  processor.finishPostProcessingStage()

  }

  /**
    * This method is to build an interphase between ODS to DWH where it write ODS Load details into Kafka TOPIC
    */
  private def loadOdsToDwhKafkaMessage(ds: Dataset[T_NdhOds], event: SupplierRecordsFile, eventDescription: String): Unit = {
    //appContext.logger.logMessage(s"Starting ODS to DWH Kafka message writing for the ODS load event $event")

    val srcImplPitId = ProcessDwhStreamConstants.SRC_SYS_CD_ODS
    val srcProcPitId = ProcessDwhStreamConstants.SRC_SYS_CD_ODS_PROC_SOURCE
    val dataTypShrtNm = event.tabToValidate

   /* val dsuppProcId = ds.select("dervd_dsupp_proc_id")
    val dervdDsuppFileFreqTypCd = dsuppProcId match {
      case "AMGROS" => "DAILY"
      case _ => "MONTHLY"
    }*/

    val dervdDsuppFileFreqTypCd = ds.select("dervd_pd_lvl_cd").distinct().collectAsList().get(0).getInt(0).toString match {
      case ProcessDwhStreamConstants.DAILY_PERIOD_LEVEL_ID => ProcessDwhStreamConstants.DAILY_PERIOD_LEVEL_CODE
      case ProcessDwhStreamConstants.WEEKLY_PERIOD_LEVEL_ID => ProcessDwhStreamConstants.WEEKLY_PERIOD_LEVEL_CODE
      case ProcessDwhStreamConstants.MONTHLY_PERIOD_LEVEL_ID => ProcessDwhStreamConstants.MONTHLY_PERIOD_LEVEL_CODE
      case ProcessDwhStreamConstants.QUARTERLY_PERIOD_LEVEL_ID => ProcessDwhStreamConstants.QUARTERLY_PERIOD_LEVEL_CODE
      case ProcessDwhStreamConstants.HALFYEARLY_PERIOD_LEVEL_ID => ProcessDwhStreamConstants.HALFYEARLY_PERIOD_LEVEL_CODE
      case _ => "INVALID"
    }

    val dervdDsuppFileTmPdNbr = new TimestampConverter(new Timestamp(event.ts)).notTsUTCText("yyyyMMdd")

    val df = ds.withColumn("data_typ_shrt_nm",lit(dataTypShrtNm))
               .withColumn("dervd_dsupp_file_freq_typ_cd",lit(dervdDsuppFileFreqTypCd))
               .withColumn("src_impl_pit_id",lit(srcImplPitId))
               .withColumn("dervd_dsupp_file_tm_pd_nbr",lit(dervdDsuppFileTmPdNbr))
               .withColumn("src_proc_pit_id",lit(srcProcPitId)).toDF()

    val odsLoadedEventDf = df.select(col("src_impl_pit_id"),
      col("src_proc_pit_id"),
      col("data_typ_shrt_nm"),
      col("src_sys_btch_id").alias("impl_btch_pit_id"),
      col("dervd_dsupp_proc_id").alias("supld_dsupp_proc_id"),
      col("dervd_dsupp_file_freq_typ_cd"),
      col("dervd_dsupp_file_tm_pd_nbr").cast("Int"),
      col("proc_eff_ts")
    )

    val odsTransformedEventDf = odsLoadedEventDf
      .withColumn("ops_tbl_isrted_ts", unix_timestamp(odsLoadedEventDf("proc_eff_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS").multiply(1000).cast(LongType))
      .withColumn("ops_topic_isrted_ts", lit(new TimestampConverter().millis))
      .drop("proc_eff_ts")

    val odsToDwhLoadEventJson = s"""${odsTransformedEventDf.distinct().toJSON.collect().mkString(",")}"""
    println(s"Kafka message is $odsToDwhLoadEventJson")
    sparkExecutor.kafka.sendEventToKafka(odsToDwhLoadEventJson, eventDescription, odsReadyTopic)
  }
}
