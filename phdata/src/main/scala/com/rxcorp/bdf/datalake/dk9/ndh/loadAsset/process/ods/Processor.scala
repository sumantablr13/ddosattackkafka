package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.ods

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.SparkExecutor
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, BemConf, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.spark.DaqeSSTTbl2
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical.NdhOds
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark.{DspnsdRxTransAcq, NdhTransSumAcq}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.SupplierRecordsFile
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.ValidationRunner
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.ods.OdsEmailStats
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.DataFormatValidation.T_NdhOds

/**
  * Created by DJha on 12/05/2018.
  */
case class Processor(pipeLineName: String, appContext: AppContext, sparkExecutor: SparkExecutor) {

  /*val logStageName: String = "PostProcessing"
  var logStagePath = ""
  var logStepDesc = ""
  var logRulePath = ""
  var logStepName = ""
  var ruleNumber = 0*/

  /*def startPostProcessingStage() = {
    logStagePath = appContext.logger.startStage(logStageName, stageNumber)
  }

  def finishPostProcessingStage() = {
    appContext.logger.finishStage(logStagePath,stageDescriptionVal = "Loading ODS records into Datalalke")
  }*/

  def loadOdsTable(ds: Dataset[T_NdhOds], event:SupplierRecordsFile) = {


    val table: NdhTransSumAcq  = new NdhTransSumAcq(appContext)
    val df = ds
      .select(table.schema.map(field => col(field.name)): _*)
      .toDF().coalesce(2)

    df.show(4, truncate = false)
    /*OdsEmailStats.odsFactTblCnt = df.count()
    OdsEmailStats.totDspnsdUnits = df.agg(sum("dspnsd_qty").cast("decimal(38,4)")).na.fill(0).head().getDecimal(0)
    OdsEmailStats.totPscbrUnits = df.agg(sum("pscr_qty").cast("decimal(38,4)")).na.fill(0).head().getDecimal(0)*/

    def insertToTable(dataFrame: DataFrame) = {
      sparkExecutor.sql.insertTable(
        table,
        dataFrame,
        "Loading final data into ODS Fact table",
        "SB",
        df.select("dervd_dsupp_proc_id").head(0).mkString,
        new TimestampConverter()
      )
      sparkExecutor.sql.refreshTable(table)  // refreshing metadata
    }
  //  val procEffDt = df.select("proc_eff_dt").head.getInt(0)
  //  val dervdDsuppProcId = df.select("dervd_dsupp_proc_id").head().mkString
 //   val oprSupldPdId = df.select("opr_supld_pd_id").head.getInt(0)
    /* appContext.logger.logMetadata(fileNameVal = event.dervdSupplierFileNm,methodNameVal = s"${table.databaseName}.${table.physicalName}",recordCountVal = df.count().toString,fileSize = sparkExecutor.file.isFileValid(event.path)._2.toString,insertedDate = creDayNbr.toString)
     appContext.logger.removeMetadataLogs()*/

    insertToTable(df)
//    val validationRunner= new ValidationRunner()
//    val impalaRefreshStatus = validationRunner.refreshImpalaStats(appContext, new com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.impala.NdhTransSumAcq(appContext), table.helperCreatePartitionSpec(oprSupldPdId))

//    impalaRefreshStatus
    false
  }
}
