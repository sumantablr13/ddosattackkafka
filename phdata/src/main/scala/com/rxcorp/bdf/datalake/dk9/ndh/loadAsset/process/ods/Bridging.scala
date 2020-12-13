/*
package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.ods

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.SparkExecutor
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SparkSqlQuery
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, BemConf}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical.NdhOds
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.spark.RdsLocationCode
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.SupplierRecordsFile
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.ods.ProcessDcpStreamConstants
import com.rxcorp.bdf.logging.log._
import org.apache.spark.sql.{DataFrame, Dataset}

case class Bridging( pipeLineName: String
                     , stageNumber: Int
                     , appContext: AppContext
                     , sparkExecutor: SparkExecutor
                     , event:SupplierRecordsFile
                     , procInd: String = ""
                     , suppressBemMsg: Boolean = false
                   ) {

  private val logStageName: String = "Bridging"
  private var logStagePath = ""
  private var logStepDesc = ""
  private var logRulePath = ""
  private var logStepName = ""
  private var ruleNumber = 0

  private val pathVal = {
    val HDFS_NAME_NODE = appContext.config.getString("HDFS_NAME_NODE").trim
    val HDFS_USER_BASE = appContext.config.getString("HDFS_USER_BASE").trim
    val ENV_USER = appContext.config.getString("ENV_USER").trim
    val kfPwd = appContext.config.getString("KF_PWD").trim
    s"$HDFS_NAME_NODE$HDFS_USER_BASE/$ENV_USER/$kfPwd"
  }

  private val jdbcpwd = sparkExecutor.file.getPwd(pathVal).trim
  private val jdbcuser = appContext.config.getString("KF_USER").trim

  private val jdbcUrl = {
    val jdbcHostname = appContext.config.getString("KF_SERVER").trim
    val jdbcPort = appContext.config.getString("SQL_PORT").trim
    val jdbcDatabase = appContext.config.getString("KF_DATABASE").trim
    s"jdbc:sqlserver://$jdbcHostname:$jdbcPort;database=$jdbcDatabase;user=$jdbcuser;password=$jdbcpwd"
  }

  private val jdbcUrlBT = {
    val jdbcHostnameBT = appContext.config.getString("BT_SERVER").trim
    val jdbcPort = appContext.config.getString("SQL_PORT").trim
    val jdbcDatabaseBT = appContext.config.getString("BT_DATABASE").trim
    s"jdbc:sqlserver://$jdbcHostnameBT:$jdbcPort;database=$jdbcDatabaseBT;user=$jdbcuser;password=$jdbcpwd"
  }

  def startBridgingStage(): Unit = logStagePath = appContext.logger.startStage(logStageName, stageNumber,stageDescriptionVal = s"Bridging Supplier ${event.dervdSupplierCd} to standard IQVIA format")

  def finishBridgingStage(): Unit = appContext.logger.finishStage(logStagePath, stageDescriptionVal = s"Bridging Supplier ${event.dervdSupplierCd} to standard IQVIA format")

  def locationBridging(input: Dataset[NdhOds]): Dataset[NdhOds] = {
    if (!suppressBemMsg) {
      appContext.bemLogger.logBusinessEventStart(
        eventDescription = "Location bridging has been started",
        eventOperation = BemConf.EventOperation.ODS_LOCATION_BRIDGING,
        componentIdentifier = "Bridging"
      )
    }
    var logRuleName = "LocationBridging"
    logStepDesc = s"Bridging Supplier Location information to standard IQVIA Location"
    var ruleDesc = logStepDesc
    logRulePath = appContext.logger.startRule(logStageName, logRuleName, stageNumber, ruleNumber, logStepDesc)

    val rdslocationtable: RdsLocationCode = new RdsLocationCode(appContext)

    val inputDf = input.toDF()

    val app = "Location Bridging"

    val inputODSData = inputDf.drop("dspnsing_oac_id","panel_mbr_cd","dspnsing_oac_brdgg_meth_nm", "dspnsing_oac_brdgg_stat_cd", "dspnsing_oac_ref_impl_id", "dspnsing_oac_lgcy_ref_impl_id", "dspnsing_oac_lgcy_id", "dspnsing_oac_brdgg_proc_ts")

    sparkExecutor.sql.registerAsTempTable(inputODSData, "INPUT ODS Data", "inputODSData")

    val onekeysql =s"""select dspnsing_oac_id,panel_mbr_cd,publ_eff_ts,dspnsing_oac_lgcy_id,dervd_dsupp_proc_id,supld_dspnsing_oac_id from ${rdslocationtable.tableName} """

    val locationData = sparkExecutor.sql.executeBridgingQuery(onekeysql)

    sparkExecutor.sql.registerAsTempTable(locationData, "locationBridging", "v_dspnsd_rx_oac_brdg")

    val LocationBridgingODS: SparkSqlQuery = new SparkSqlQuery() {
      override lazy val logMessage = "Location Bridging Starts"
      override lazy val sqlStatement =
        s"""
        select input.*,
           CASE WHEN nvl(trim(oac.dspnsing_oac_id),'') <> '' THEN '${ProcessDcpStreamConstants.BRDG_REF_IMPL_ID_RDS}' ELSE NULL END dspnsing_oac_ref_impl_id,
           CASE WHEN nvl(trim(oac.dspnsing_oac_lgcy_id),'') <> '' THEN '${ProcessDcpStreamConstants.BRDG_REF_IMPL_ID_RDS}' ELSE NULL END dspnsing_oac_lgcy_ref_impl_id ,
           CASE WHEN nvl(trim(oac.dspnsing_oac_id),'') <> '' THEN '${ProcessDcpStreamConstants.BRDG_METHOD_AUTO}' ELSE NULL END dspnsing_oac_brdgg_meth_nm,
           CASE WHEN nvl(trim(oac.dspnsing_oac_id),'') <> '' THEN '${ProcessDcpStreamConstants.BRDG_STATUS_BRIDGED}' ELSE '${ProcessDcpStreamConstants.BRDG_STATUS_UNBRIDGED}' END dspnsing_oac_brdgg_stat_cd,
           oac.dspnsing_oac_id,
           oac.dspnsing_oac_lgcy_id,
           oac.panel_mbr_cd,
           CASE WHEN nvl(trim(oac.dspnsing_oac_id),'') <> ''  THEN oac.publ_eff_ts ELSE null END dspnsing_oac_brdgg_proc_ts
           from inputODSData input
           LEFT JOIN v_dspnsd_rx_oac_brdg oac
           ON upper(nvl(trim(input.dervd_dsupp_proc_id),'')) = upper(trim(oac.dervd_dsupp_proc_id))
           AND upper(nvl(trim(input.supld_dspnsing_oac_id),'')) = upper(trim(oac.supld_dspnsing_oac_id))
         """.stripMargin
    }

    val LocationBridgedODSAttributes = sparkExecutor.sql.getDataFrame(LocationBridgingODS)

    val output: Dataset[NdhOds] = sparkExecutor.file.encoderTest[NdhOds](LocationBridgedODSAttributes)


    var (locbrdgdCnt, locUnbrdgdCnt) = (0: Long, 0: Long)

    val locationBridgingDetails = LocationBridgedODSAttributes.groupBy("dspnsing_oac_brdgg_stat_cd").count().take(10)

    locationBridgingDetails.map {
      f =>
        f.get(0) match {
          case ProcessDcpStreamConstants.BRDG_STATUS_BRIDGED => locbrdgdCnt += f.getLong(1) //println(f.get(1))
          case ProcessDcpStreamConstants.BRDG_STATUS_UNBRIDGED => locUnbrdgdCnt += f.getLong(1)
          case _ => 0
        }
    }

    logStepName = "LocationBridging"
    var stepNumber = 0
    logStepName = "LocationBridging_Bridged"
    logStepDesc = "Location Bridged Count"
    var logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$locbrdgdCnt")
    appContext.logger.logMessage("Location Bridged Count")

    logStepName = "LocationBridging_Unbridged"
    logStepDesc = "Location Unbridged Count"
    stepNumber += 1
    logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$locUnbrdgdCnt")
    appContext.logger.logMessage("Location Unbridged Count")

    /* QC0 logging for Location unbridged */
    val tags = Map(
      countryScope.name -> appContext.config.getString("ENV_TENANT_CODE_SHRT").trim,
      assetTypeScope.name -> appContext.config.getString("ENV_APPLICATION_DESC").trim.toUpperCase,
      serviceScope.name -> appContext.envApplicationCode,
      supplierScope.name -> event.dervdSupplierCd,
      dataType.name -> "FILE",
      "fileName" -> event.dervdSupplierFileNm
    ) ++ Map("fieldName" -> "dspnsing_oac")

    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_bridged", locbrdgdCnt, tags, "success")
    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_unbridged", locUnbrdgdCnt, tags, "success")
    /* QC0 logging for Location unbridged */

    appContext.logger.finishRule(logRulePath, recordCountVal = output.count.toString, ruleDescriptionVal = ruleDesc)
    ruleNumber += 1

    if (!suppressBemMsg) {
      appContext.bemLogger.logBusinessEventFinish(
        eventDescription =
          s"Product bridging has been completed successfully for file ${event.dervdSupplierFileNm}. " +
            s"Bridged record count: ${locbrdgdCnt.toString}, unbridged record count: ${locUnbrdgdCnt.toString}",
        eventOutcome = BemConf.EventOutcome.SUCCESS
      )
    }
    output
  }

  def productBridging(input:DataFrame): DataFrame = {
    //todo: kibana log need to verify
    if (!suppressBemMsg) {
      appContext.bemLogger.logBusinessEventStart(
        eventDescription = "Product bridging has been started",
        eventOperation = BemConf.EventOperation.ODS_PRODUCT_BRIDGING,
        componentIdentifier = "Bridging"
      )
    }
    val logRuleName: String = "ProductBridging"
    logStepDesc = "Bridging Product information to standard IQVIA Product"
    logRulePath = appContext.logger.startRule(logStageName, logRuleName, stageNumber, ruleNumber, logStepDesc)
    //Step 1 Start
    logStepName = "ProductBridging"
    logStepDesc = "Bridging Supplier Product completed"
    var stepNumber = 0
    val inputddd = input.drop("pscr_fcc_brdgg_stat_cd", "pscr_fcc_brdgg_meth_nm", /*"pscr_fcc_brdgg_accrcy_pct",*/ "pscr_fcc_brdgg_proc_ts", "pscr_fcc_ref_impl_id", "pscr_fcc_id", "dspnsd_fcc_brdgg_stat_cd", "dspnsd_fcc_brdgg_meth_nm", /*"dspnsd_fcc_brdgg_accrcy_pct",*/ "dspnsd_fcc_brdgg_proc_ts", "dspnsd_fcc_ref_impl_id", "dspnsd_fcc_id")
    sparkExecutor.sql.registerAsTempTable(inputddd.toDF(),"Bridging Input Data", "InputODS")

    val app = "Product Bridging"
    val pscrViewName = appContext.config.getString("PSCR_VIEW_NAME").trim
    val dspnsdViewName = appContext.config.getString("DSPNSD_VIEW_NAME").trim

    val pscrSql =
      s"""
         |SELECT
         |  intFCC,
         |  strBridgeStatus,
         |  datUpdateDate,
         |  strDSProdDesc
         |FROM $pscrViewName
       """.stripMargin

    val dspnsdSql =
      s"""
         |SELECT
         |  intFCC,
         |  strBridgeStatus,
         |  datUpdateDate,
         |  strDSProdDesc
         |FROM $dspnsdViewName
       """.stripMargin

    val pscroptions = Map[String, String](
      "jdbcUser" -> s"$jdbcuser",
      "jdbcPwd" -> s"$jdbcpwd",
      "jdbcSql" -> s"$pscrSql",
      "jdbcUrl" -> s"$jdbcUrlBT"
    )

    val dspnsdoptions = Map[String, String](
      "jdbcUser" -> s"$jdbcuser",
      "jdbcPwd" -> s"$jdbcpwd",
      "jdbcSql" -> s"$dspnsdSql",
      "jdbcUrl" -> s"$jdbcUrlBT"
    )

    val pscrDf = sparkExecutor.sql.createJdbcSqlConnection(pscroptions)
    val dspnsdDf = sparkExecutor.sql.createJdbcSqlConnection(dspnsdoptions)
    sparkExecutor.sql.registerAsTempTable(pscrDf, "Prescribed Products", "BridgedPrescribedProducts")
    sparkExecutor.sql.registerAsTempTable(dspnsdDf, "Dispensed Products", "BridgedDispensedProducts")
    val prodBridgedODS: SparkSqlQuery = new SparkSqlQuery() {
      override lazy val logMessage = "Product Bridging Starts"
      override lazy val sqlStatement =
        s"""
           |SELECT
           |  input.*,
           |  CASE WHEN (pscrBrd.strBridgeStatus = 'B') OR (pscrBrd.strBridgeStatus = 'I') OR (pscrBrd.strBridgeStatus = 'U') THEN '${ProcessDcpStreamConstants.BRDG_REF_IMPL_ID_KFBT}'
           |    ELSE NULL
           |  END  AS pscr_fcc_ref_impl_id,
           |    CASE WHEN (pscrBrd.strBridgeStatus = 'B') OR (pscrBrd.strBridgeStatus = 'I') OR (pscrBrd.strBridgeStatus = 'U') THEN '${ProcessDcpStreamConstants.BRDG_METHOD_AUTO}'
           |      ELSE NULL
           |  END AS pscr_fcc_brdgg_meth_nm,
           |  CASE WHEN pscrBrd.strBridgeStatus = 'I' THEN '${ProcessDcpStreamConstants.BRDG_STATUS_IGNORED}'
           |    WHEN pscrBrd.strBridgeStatus = 'U' THEN '${ProcessDcpStreamConstants.BRDG_STATUS_UNKNOWN}'
           |    WHEN pscrBrd.strBridgeStatus = 'B' THEN '${ProcessDcpStreamConstants.BRDG_STATUS_BRIDGED}'
           |    ELSE '${ProcessDcpStreamConstants.BRDG_STATUS_UNBRIDGED}'
           |  END AS pscr_fcc_brdgg_stat_cd,
           |  pscrBrd.intFCC AS pscr_fcc_id,
           |  CASE WHEN (pscrBrd.strBridgeStatus = 'B') OR (pscrBrd.strBridgeStatus = 'I') OR (pscrBrd.strBridgeStatus = 'U') THEN from_unixtime(unix_timestamp(nvl(trim(pscrBrd.datUpdateDate), ''), 'yyyy-MM-dd HH:mm:ss'))
           |    ELSE null
           |  END AS pscr_fcc_brdgg_proc_ts,
           |  CASE WHEN (dspnsdBrd.strBridgeStatus = 'B') OR (dspnsdBrd.strBridgeStatus = 'I') OR (dspnsdBrd.strBridgeStatus = 'U') THEN '${ProcessDcpStreamConstants.BRDG_REF_IMPL_ID_KFBT}'
           |    ELSE NULL
           |  END  AS dspnsd_fcc_ref_impl_id,
           |  CASE WHEN (dspnsdBrd.strBridgeStatus = 'B') OR (dspnsdBrd.strBridgeStatus = 'I') OR (dspnsdBrd.strBridgeStatus = 'U') THEN '${ProcessDcpStreamConstants.BRDG_METHOD_AUTO}'
           |    ELSE NULL
           |  END AS dspnsd_fcc_brdgg_meth_nm,
           |  CASE WHEN dspnsdBrd.strBridgeStatus = 'I' THEN '${ProcessDcpStreamConstants.BRDG_STATUS_IGNORED}'
           |    WHEN dspnsdBrd.strBridgeStatus = 'U' THEN '${ProcessDcpStreamConstants.BRDG_STATUS_UNKNOWN}'
           |    WHEN dspnsdBrd.strBridgeStatus = 'B' THEN '${ProcessDcpStreamConstants.BRDG_STATUS_BRIDGED}'
           |    ELSE '${ProcessDcpStreamConstants.BRDG_STATUS_UNBRIDGED}'
           |  END AS dspnsd_fcc_brdgg_stat_cd,
           |  dspnsdBrd.intFCC as dspnsd_fcc_id,
           |  CASE WHEN (dspnsdBrd.strBridgeStatus = 'B') OR (dspnsdBrd.strBridgeStatus = 'I') OR (dspnsdBrd.strBridgeStatus = 'U') THEN from_unixtime(unix_timestamp(nvl(trim(dspnsdBrd.datUpdateDate), ''), 'yyyy-MM-dd HH:mm:ss'))
           |    ELSE null
           |  END AS dspnsd_fcc_brdgg_proc_ts
           |FROM InputODS input
           |LEFT JOIN BridgedPrescribedProducts pscrBrd
           |  ON upper(nvl(trim(input.nrmlz_pscr_pcmdty_desc),'')) = upper(trim(pscrBrd.strDSProdDesc))
           |LEFT JOIN BridgedDispensedProducts dspnsdBrd
           |  ON upper(nvl(trim(input.nrmlz_dspnsd_pcmdty_desc),'')) = upper(trim(dspnsdBrd.strDSProdDesc))
         """.stripMargin
    }


    val ProductBridgedODSAttributes = sparkExecutor.sql.getDataFrame(prodBridgedODS)
    //var ProductBridgedODSAttributes = sparkExecutor.sql.emptyDatasetString.toDF()

    //ProductBridgedODSAttributes = ProductBridgedODSAttributes_res.drop(col("ddd_si_trans_unit_qty")).withColumn("ddd_si_trans_unit_qty",ProductBridgedODSAttributes_res("tmp_ddd_si_trans_unit_qty").cast(LongType)).drop("tmp_ddd_si_trans_unit_qty")

    //val output: Dataset[PbsOds] = sparkExecutor.file.encoderTest[PbsOds](ProductBridgedODSAttributes)

    val presribedBridgingDetails = ProductBridgedODSAttributes.groupBy("pscr_fcc_brdgg_stat_cd").count().take(10)
    //TODO: This process need improvemnet.
    var (pscrBridgedRecords, pscrUnbridgedProducts, pscrIgnoredProducts, pscrUnknownProducts) = (0:Long, 0:Long, 0:Long, 0:Long)
    presribedBridgingDetails.map{
      f=>f.get(0)  match {
        case ProcessDcpStreamConstants.BRDG_STATUS_BRIDGED => pscrBridgedRecords += f.getLong(1)//println(f.get(1))
        case ProcessDcpStreamConstants.BRDG_STATUS_UNBRIDGED => pscrUnbridgedProducts += f.getLong(1)
        case ProcessDcpStreamConstants.BRDG_STATUS_IGNORED => pscrIgnoredProducts += f.getLong(1)
        case ProcessDcpStreamConstants.BRDG_STATUS_UNKNOWN => pscrUnknownProducts += f.getLong(1)
        case _ =>0
      }
    }

    val dispensedBridgingDetails = ProductBridgedODSAttributes.groupBy("dspnsd_fcc_brdgg_stat_cd").count().take(10)
    //TODO: This process need improvement
    var (dspnsdBridgedRecords, dspnsdUnbridgedProducts, dspnsdIgnoredProducts, dspnsdUnknownProducts) = (0:Long, 0:Long, 0:Long, 0:Long)
    dispensedBridgingDetails.map{
      t=>t.get(0)  match {
        case ProcessDcpStreamConstants.BRDG_STATUS_BRIDGED => dspnsdBridgedRecords += t.getLong(1)//println(f.get(1))
        case ProcessDcpStreamConstants.BRDG_STATUS_UNBRIDGED => dspnsdUnbridgedProducts += t.getLong(1)
        case ProcessDcpStreamConstants.BRDG_STATUS_IGNORED => dspnsdIgnoredProducts += t.getLong(1)
        case ProcessDcpStreamConstants.BRDG_STATUS_UNKNOWN => dspnsdUnknownProducts += t.getLong(1)
        case _ =>0
      }
    }

    logStepName = "ProductPrescribedBridging_Bridged"
    logStepDesc = "Product Prescribed Bridged Count"
    stepNumber = 0
    var logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)

    appContext.logger.logMessage("Prescribed Bridged Product Count")
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$pscrBridgedRecords")

    logStepName = "ProductPrescribedBridging_Unbridged"
    logStepDesc = "Product Prescribed Unbridged Count"
    stepNumber += 1
    logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)

    appContext.logger.logMessage("Prescribed UnBridged Product Count")
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$pscrUnbridgedProducts")

    logStepName = "ProductPrescribedBridging_Unknown"
    logStepDesc = "Product Prescribed Unknown Count"
    stepNumber += 1
    logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)

    appContext.logger.logMessage("Prescribed Unknown Product Count")
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$pscrIgnoredProducts")

    logStepName = "ProductPrescribedBridging_Ignore"
    logStepDesc = "Product Prescribed Ignore Count"
    stepNumber += 1
    logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)

    appContext.logger.logMessage("Prescribed Ignore Product Count")
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$pscrUnknownProducts")

    logStepName = "ProductDispensedBridging_Bridged"
    logStepDesc = "Product Dispensed Bridged Count"
    stepNumber += 1
    logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)

    appContext.logger.logMessage(" Dispensed Bridged Product Count")
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$dspnsdBridgedRecords")

    logStepName = "ProductDispensedBridging_Unbridged"
    logStepDesc = "Product Dispensed Unbridged Count"
    stepNumber += 1
    logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)

    appContext.logger.logMessage("Dippensed UnBridged Product Count")
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$dspnsdUnbridgedProducts")

    logStepName = "ProductDispensedBridging_Unknown"
    logStepDesc = "Product Dispensed Unknown Count"
    stepNumber += 1
    logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)

    appContext.logger.logMessage("Dispensed Unknown Product Count")
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$dspnsdIgnoredProducts")

    logStepName = "ProductDispensedBridging_Ignore"
    logStepDesc = "Product Dispensed Ignore Count"
    stepNumber += 1
    logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)

    appContext.logger.logMessage("Dispensed Ignore Product Count")
    appContext.logger.finishStep(logStepPath, recordCountVal = s"$dspnsdUnknownProducts")

    //QC0 logging for Product bridging
    val tags = Map(
      countryScope.name     -> appContext.config.getString("ENV_TENANT_CODE_SHRT").trim,
      assetTypeScope.name   -> appContext.config.getString("ENV_APPLICATION_DESC").trim.toUpperCase,
      serviceScope.name     -> appContext.envApplicationCode,
      supplierScope.name    -> event.dervdSupplierCd,
      dataType.name         -> "FILE",
      "fileName"            -> event.dervdSupplierFileNm
    ) ++ Map("fieldName" -> "nrmlz_pscr_pcmdty_desc","fieldName1"->"nrmlz_dspnsd_pcmdty_desc")

    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_bridged",pscrBridgedRecords,tags, "success")
    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_unbridged",pscrUnbridgedProducts,tags, "success")
    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_ignore",pscrIgnoredProducts,tags, "success")
    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_unknown",pscrUnknownProducts,tags, "success")
    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_bridged",dspnsdBridgedRecords,tags, "success")
    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_unbridged",dspnsdUnbridgedProducts,tags, "success")
    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_ignore",dspnsdIgnoredProducts,tags, "success")
    appContext.qcLogger.qc0Logger("Events for QC0", "count_by_unknown",dspnsdUnknownProducts,tags, "success")

    //end QC0 logging for Products

    val ruleDesc="Bridging Product information to standard IQVIA Product"
    appContext.logger.finishRule(logRulePath, recordCountVal = ProductBridgedODSAttributes.count.toString,ruleDescriptionVal = ruleDesc)
    ruleNumber += 1

    if (!suppressBemMsg) {
      appContext.bemLogger.logBusinessEventFinish(
        eventDescription =
          s"Product bridging has been completed successfully for file ${event.dervdSupplierFileNm}. " +
            s"Bridged Dispensed record count: ${dspnsdBridgedRecords}, Bridged Prescribed record count: ${pscrBridgedRecords}",
        eventOutcome = BemConf.EventOutcome.SUCCESS
      )
    }
    ProductBridgedODSAttributes
  }

  /* def depotBridging(input: Dataset[PbsOds]): Dataset[PbsOds] = {
     var logRuleName: String = "DepotBridging"
     logStepDesc = "Bridging Depot information to standard IQVIA Depot"
     var ruleDesc=logStepDesc
     logRulePath = appContext.logger.startRule(logStageName, logRuleName, stageNumber, ruleNumber, logStepDesc)
     logStepName = "DepotBridging"
     var stepNumber = 0

     val inputddd = input.drop("provdng_oac_depot_ref_impl_id", "provdng_oac_depot_ref_proc_ts", "provdng_oac_depot_brdg_accrcy_pct", "provdng_oac_depot_brdg_meth_nm", "provdng_oac_depot_brdg_err_cd", "provdng_oac_depot_id", "dsupp_proc_id")

     sparkExecutor.sql.registerAsTempTable(inputddd.toDF(), "Bridging Input Data", "InputODS")

     val uniqueSupplierCd = new SparkSqlQuery {
       override val sqlStatement: String =
         s"""select distinct(dervd_dsupp_proc_id) supplierCd from InputODS"""
       override val logMessage: String = "Getting Unique Supplier Codes"
     }

     val supplierCds = event.dervdSupplierCd.toInt

     val app = "Depot Bridging"
     val sql =  s"""select distinct DepotNum as supplier_depot_cd,IMSDepotNum as ims_supplier_cd,DSGroupID as supplier_cd,DSGroupDesc as supplier_desc,
               IMSDepotNum as supplier_distng_oac_depot_cd,DepotDesc as supplier_distng_oac_depot_desc,
               ProductionFlag as production_flag from dst.bridgedist_bdf where DSGroupID=$supplierCds""".stripMargin

     val options = Map[String, String](
       "jdbcUser" -> s"$jdbcuser",
       "jdbcPwd" -> s"$jdbcpwd",
       "jdbcSql" -> s"$sql",
       "jdbcUrl" -> s"$jdbcUrl"
     )

     val depotref = sparkExecutor.sql.createJdbcSqlConnection(options)
     sparkExecutor.sql.registerAsTempTable(depotref, "Depots Reference Data", "BridgedDepots")

     var supplierBrdCondition="b.supplier_depot_cd"
     if(event.supplierId.equals("4002") && event.dervdSupplierFilePdLvl.equalsIgnoreCase("W")){
       supplierBrdCondition="b.ims_supplier_cd"
     }

     appContext.logger.logMessage(s"Join condition for depo bridging $supplierBrdCondition")
     val DepotBridgedODS: SparkSqlQuery = new SparkSqlQuery() {
       override lazy val logMessage = "Depot Bridging Starts"
       override lazy val sqlStatement =
         s"""
            |SELECT  a.*,
            |CASE nvl(b.supplier_distng_oac_depot_cd,'') WHEN '' then NULL ELSE 'KF' end  as provdng_oac_depot_ref_impl_id,
            | ' ' as provdng_oac_depot_ref_proc_ts,
            |CASE nvl(b.supplier_distng_oac_depot_cd,'') WHEN '' then 0 ELSE  1.00 end as provdng_oac_depot_brdg_accrcy_pct,
            |case nvl(b.supplier_distng_oac_depot_cd,'') WHEN '' then NULL ELSE 'Auto' end as provdng_oac_depot_brdg_meth_nm,
            | CASE nvl(b.supplier_distng_oac_depot_cd,'') WHEN '' then 'Unknown' ELSE  'Bridged' end AS provdng_oac_depot_brdg_err_cd,
            |b.supplier_distng_oac_depot_cd as provdng_oac_depot_id,
            |b.supplier_cd as dsupp_proc_id
            | FROM InputODS a
            |LEFT JOIN BridgedDepots b ON trim(a.dervd_dsupp_proc_id) = trim(b.supplier_cd) AND cast(nvl(trim(a.nrmlz_provdng_oac_depot_id),'0') as int) =trim($supplierBrdCondition) and b.production_flag = 'A'
       """.stripMargin
     }

     val DepotBridgedODSAttributes = sparkExecutor.sql.getDataFrame(DepotBridgedODS)//.repartition(10)//.cache()
     val output: Dataset[PbsOds] = sparkExecutor.file.encoderTest[PbsOds](DepotBridgedODSAttributes)

     val depoBridgingDetails= DepotBridgedODSAttributes.groupBy("provdng_oac_depot_brdg_err_cd").count().take(10)
     var (bridgedrecords,unknownRecords) = (0:Long,0:Long)
     depoBridgingDetails.map{
       f=>f.get(0)  match {
         case "Bridged"|"B" => bridgedrecords+=f.getLong(1)//println(f.get(1))
         case "Unknown"|"U" =>unknownRecords+=f.getLong(1)
         case _ =>0
       }
     }

     logStepName = "DepotBridging_Bridged"
     logStepDesc = "Depot Bridged Count"
     stepNumber = 0
     var logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)
     appContext.logger.logMessage("Depot Bridged Count")
     appContext.logger.finishStep(logStepPath, recordCountVal = s"$bridgedrecords")
     logStepName = "DepotBridging_Unknown"
     logStepDesc = "Depot Unknown Count"
     stepNumber += 1
     logStepPath = appContext.logger.startStep(logStageName, logRuleName, logStepName, stageNumber, ruleNumber, stepNumber, logStepDesc)
     appContext.logger.logMessage(" Depot Unknown Count")
     appContext.logger.finishStep(logStepPath, recordCountVal = s"$unknownRecords")

     appContext.logger.finishRule(logRulePath, recordCountVal = output.count.toString,ruleDescriptionVal = ruleDesc)
     ruleNumber += 1
     //QC0 logging for Depot bridging
     val tags = Map(
       countryScope.name     -> appContext.config.getString("ENV_TENANT_CODE_SHRT").trim,
       assetTypeScope.name   -> appContext.config.getString("ENV_APPLICATION_DESC").trim.toUpperCase,
       serviceScope.name     -> appContext.envApplicationCode,
       supplierScope.name    -> event.dervdSupplierCd,
       dataType.name         -> "FILE",
       "fileName"            -> event.dervdSupplierFileNm
     ) ++ Map("fieldName" -> "provdng_oac_depot_id")

     appContext.qcLogger.qc0Logger("Events for QC0", "count_by_bridged",bridgedrecords,tags, "success")
     //appContext.qcLogger.qc0Logger("Events for QC0", "count_by_unbridged",UnknownDepots,tags, "SUCCESS")
     appContext.qcLogger.qc0Logger("Events for QC0", "count_by_unknown",unknownRecords,tags, "success")

     //end QC0 logging for Depot

     output
   }*/

  def apply( pipeLineName: String
             , stageNumber: Int
             , appContext: AppContext
             , event: SupplierRecordsFile
           ): Bridging = Bridging( pipeLineName, stageNumber, appContext, sparkExecutor,event )
}*/
