//package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.ods
//
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{KafkaOffsetPersistance, TimestampConverter}
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.{SparkExecutor, TestSparkExecutor}
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.prodcalendar.ProdCalendarInfo
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dcp.logical.DcpSupplierReadyEvent
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark.{TimeDyDim, TimeWkDim}
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark.{DspnsdRxTransAcq, DsuppDspnsdRxDetl, DsuppDspnsdRxHdr, PbsFactOdsErrLog}
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.spark.RdsLocationCode
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.SupplierRecordsFile
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.{EmailAlerts, FileContentValidation, HeaderValidation, ValidationRunner}
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test.NdhUnitTest
//import org.apache.spark.sql._
//import org.mockito.Matchers._
//import org.mockito.Mockito._
//import org.mockito.invocation.InvocationOnMock
//import org.mockito.stubbing.Answer
//
//import scala.io.Source
//
///**
//  * Unit test for ProcessDcpStream class
//  */
//class SuiteProcessDcpStream_FileFormatVersion1 extends NdhUnitTest {
//
//  // test starts
//  behavior of "ProcessOdsDcpStream"
//  lazy val stage = "ProcessOdsDcpStream test"
//  @transient private var sparkExecutor: SparkExecutor = _
//  @transient private var spark: SparkSession = _
//  @transient private var failureCount: Long = 0
//  @transient private var rdslocationtable: RdsLocationCode = _
//  @transient private var timePeriodDy: TimeDyDim = _
//  @transient private var timePeriodWk: TimeWkDim = _
//
//  val mockedService = mock[TestProcessDcpStream]
//  val uuid1 = s"${java.util.UUID.randomUUID()}".split('-').map(_.trim).mkString
//
//  override def beforeAll() {
//    super.beforeAll()
//    val defaultFileContentValidations = Seq(
//      FileContentValidation.NumericDataTypeValidation(appContext),
//      //      FileContentValidation.BlankTransDateValidation(appContext)
//      //            FileContentValidation.NumericDataTypeValidation(appContext),
//      // FileContentValidation.DecimalDataTypeValidation(appContext),
//      //FileContentValidation.DecimalDataTypeWithCommaValidation(appContext),
//      FileContentValidation.ZeroValueValidation(appContext),
//      //      //FileContentValidation.QuantityNumericDataTypeValidation(appContext),
//      FileContentValidation.NegativeValueValidation(appContext),
//      FileContentValidation.EmptyValueValidation(appContext)
//      //      FileContentValidation.DayNbrValidation(appContext),
//      //      FileContentValidation.MonthNbrValidation(appContext),
//      //      FileContentValidation.YearNbrValidation(appContext),
//      //      FileContentValidation.BlankClientCodeValidation(appContext),
//      //      FileContentValidation.QuantityDecimalDataTypeValidation(appContext)
//      //      FileContentValidation.BlankTransDateValidation(appContext)
//    )
//
//
//    val defaultHeaderValidations = Seq(
//      //      HeaderValidation.DayNbrValidation(appContext),
//      //      HeaderValidation.WeekNbrValidation(appContext),
//      //      HeaderValidation.MonthNbrValidation(appContext),
//      //      HeaderValidation.YearNbrValidation(appContext)
//      // HeaderValidation.NumericDataTypeValidation(appContext)
//    )
//
//    val defaultValidationsRunner = new ValidationRunner(defaultFileContentValidations, defaultHeaderValidations)
//    when(mockedService.defaultValidationsRunner).thenReturn(defaultValidationsRunner)
//    sparkExecutor = appContext.getSparkExecutor(appContext.config, appContext.logger)
//
//    val mDataTblLoadLog = sparkExecutor.sql.getMetadataTableLoadLog(appContext.assetDatabase)
//    sparkExecutor.sql.dropTable(mDataTblLoadLog)
//    sparkExecutor.sql.createTable(mDataTblLoadLog)
//
//    val kafkaTable = new MDataKafkaOffset(appContext)
//    sparkExecutor.sql.dropTable(kafkaTable)
//    sparkExecutor.sql.createTable(kafkaTable)
//
//    spark = sparkExecutor.asInstanceOf[TestSparkExecutor].getSparkSession()
//
//    //For OneKeyDb
//
//    val oneKeyDbName = appContext.config.getString("GB9_DATABASE")
//
//    rdslocationtable = new RdsLocationCode(appContext)
//
//    sparkExecutor.sql.dropTable(rdslocationtable)
//    sparkExecutor.sql.createTable(rdslocationtable)
//    val rdsInputStream = this.getClass.getResourceAsStream("/data/rds/rds_locationtable_lkp.csv")
//    val rdsLocationLines = Source.fromInputStream(rdsInputStream).getLines.toSeq
//    val rdsLocationLinesDS: Dataset[String] = spark.createDataset[String](rdsLocationLines)(Encoders.STRING)
//    val rdsLocationLinesDf: DataFrame = spark.read
//      .option("header", "true")
//      .option("delimiter", ",")
//      .option("mode", "FAILFAST")
//      .option("nullValue", "null")
//      .csv(rdsLocationLinesDS)
//
//    sparkExecutor.sql.insertOverwriteTable(rdslocationtable, rdsLocationLinesDf, "populating devl_gb9.v_oac_extn_attr_value", appContext.envApplicationCode, "", new TimestampConverter())
//
//    //Daily time period
////    timePeriodDy = new TimeDyDim(appContext)
////    sparkExecutor.sql.dropTable(timePeriodDy)
////    sparkExecutor.sql.createTable(timePeriodDy)
//
////    val timePeriodDyInfo= this.getClass.getResourceAsStream("/data/dwh/outputs/v_tm_pd_gre_day_dim.csv")
////    val timePeriodDyLines = Source.fromInputStream(timePeriodDyInfo).getLines.toSeq
////    val timePeriodDyDS = spark.createDataset[String](timePeriodDyLines)(Encoders.STRING)
////    timePeriodDyDS.show(false)
////
////    val timePeriodDyDf = spark.read
////      .option("header", value = true)
////      .option("delimiter", "|")
////      .option("mode", "FAILFAST")
////      .option("nullValue", "null")
////      .csv(timePeriodDyDS)
////    timePeriodDyDf.show(false)
////
////    if (timePeriodDyDf.count() == 0)
////      throw new Exception("ERROR: Loaded 0 records from data/dwh/outputs/v_tm_pd_gre_day_dim.csv")
////    sparkExecutor.sql.insertOverwriteTable(timePeriodDy, timePeriodDyDf, "Mocked existing source data for v_tm_pd_gre_day_dim table", appContext.envApplicationCode, "", new TimestampConverter())
////
//
//    //weekly time period
//    timePeriodWk = new TimeWkDim(appContext)
//    sparkExecutor.sql.dropTable(timePeriodWk)
//    sparkExecutor.sql.createTable(timePeriodWk)
//
//    val timePeriodWkInfo= this.getClass.getResourceAsStream("/data/dwh/outputs/v_tm_pd_gre_wk_dim.csv")
//    val timePeriodWkLines = Source.fromInputStream(timePeriodWkInfo).getLines.toSeq
//    val timePeriodWkDS = spark.createDataset[String](timePeriodWkLines)(Encoders.STRING)
//    timePeriodWkDS.show(false)
//
//    val timePeriodWkDf = spark.read
//      .option("header", value = true)
//      .option("delimiter", "|")
//      .option("mode", "FAILFAST")
//      .option("nullValue", "null")
//      .csv(timePeriodWkDS)
//    timePeriodWkDf.show(false)
//
//    if (timePeriodWkDf.count() == 0)
//      throw new Exception("ERROR: Loaded 0 records from data/dwh/outputs/v_tm_pd_gre_wk_dim.csv")
//    sparkExecutor.sql.insertOverwriteTable(timePeriodWk, timePeriodWkDf, "Mocked existing source data for v_tm_pd_gre_wk_dim table", appContext.envApplicationCode, "", new TimestampConverter())
//    spark.sql(s"SELECT * FROM ${timePeriodWk.tableName} LIMIT 10").show(2, false)
//
//    // Creating Header Storage Table
//    val dsuppDspnsdRxHdr = new DsuppDspnsdRxHdr(appContext)
//    sparkExecutor.sql.dropTable(dsuppDspnsdRxHdr)
//    sparkExecutor.sql.createTable(dsuppDspnsdRxHdr)
//
//    // Creating Error Log Table
//    val pbsFactOdsError = new PbsFactOdsErrLog(appContext)
//    sparkExecutor.sql.dropTable(pbsFactOdsError)
//    sparkExecutor.sql.createTable(pbsFactOdsError)
//
//    // Creating Ods Stage Table
//    val DsuppDspnsdRxDetl = new DsuppDspnsdRxDetl(appContext)
//    sparkExecutor.sql.dropTable(DsuppDspnsdRxDetl)
//    sparkExecutor.sql.createTable(DsuppDspnsdRxDetl)
//
//    // Creating ODS Fact Table
//    val pbsFactOds = new DspnsdRxTransAcq(appContext)
//    sparkExecutor.sql.dropTable(pbsFactOds)
//    sparkExecutor.sql.createTable(pbsFactOds)
//
//    spark.read.format("csv").load("src/test/resources/data/dcp/kf.pwd").write.format("csv").option("header", "false").mode(SaveMode.Overwrite).save(s"${appContext.config.getString("HDFS_USER_PATH")}/${appContext.config.getString("KF_PWD")}")
//    kafka.createTopic(appContext.config.getString("KAFKA_TOPIC_DWH_INPUT"))
//    kafka.createTopic(appContext.config.getString("KAFKA_TOPIC_SUPPLIER_READY"))
//    val topicName = appContext.config.getString("KAFKA_TOPIC_SUPPLIER_READY")
//    val newEvent =
//      s"""             {
//                      "Item": [
//                      {
//                      "Key": "BDFeventName",
//                      "Value": "RXSCRIPTFILEARRIVAL"
//                      },
//                      {
//                      "Key": "countryIsoCode",
//                      "Value": "GBR"
//                      },
//                      {
//                      "Key": "countryIsoName",
//                      "Value": "United Kingdom"
//                      },
//                      {
//                      "Key": "dataDomainCode",
//                      "Value": "SELLOUT"
//                      },
//                      {
//                      "Key": "dataDomain",
//                      "Value": "SELLOUT"
//                      },
//                      {
//                      "Key": "dataTypeCode",
//                      "Value": "RX"
//                      },
//                      {
//                      "Key": "dataType",
//                      "Value": " "
//                      },
//                      {
//                      "Key": "DCPsupplierId",
//                      "Value": "A00049"
//                      },
//                      {
//                      "Key": "supplierName",
//                      "Value": "Lloyds"
//                      },
//                      {
//                      "Key": "panelOrganizationIdentifier",
//                      "Value": "A00049"
//                      },
//                      {
//                      "Key": "panelOrganizationName",
//                      "Value": " "
//                      },
//                      {
//                      "Key": "parentOrganizationName",
//                      "Value": " "
//                      },
//                      {
//                      "Key": "parentOrganizationCode",
//                      "Value": " "
//                      },
//                      {
//                      "Key": "OrganizationType",
//                      "Value": " "
//                      },
//                      {
//                      "Key": "OrganizationTypeCode",
//                      "Value": " "
//                      },
//                      {
//                      "Key": "dcpFileIdentifier",
//                      "Value": "${uuid1}"
//                      },
//                      {
//                      "Key": "dcpOriginalFileName",
//                      "Value": "xLLYPMR02.830w252019.579"
//                      },
//                      {
//                      "Key": "dcpFileArrivalTime",
//                      "Value": "2019-07-08T07:42:49Z"
//                      },
//                      {
//                      "Key": "hdfsFileUrl",
//                      "Value": "src/test/resources/data/dcp/file/xLLYPMR02.830w252019.579"
//                      },
//                      {
//                      "Key": "userId",
//                      "Value": "gb9dusr"
//                      },
//                      {
//                      "Key": "inputfile",
//                      "Value": "src/test/resources/data/dcp/file/xLLYPMR02.830w252019.579"
//                      },
//                      {
//                      "Key": "fileSize",
//                      "Value": "1082980"
//                      },
//                      {
//                      "Key": "systemHouseName",
//                      "Value": "LLYPMR"
//                      },
//                      {
//                      "Key": "dataPeriod",
//                      "Value": "D"
//                      },
//                      {
//                      "Key": "requestType",
//                      "Value": "new"
//                      },
//                      {
//                      "Key": "FileFormatVersion",
//                      "Value": "FF1.0"
//                      },
//                      {
//                      "Key": "FilePeriodNumber",
//                      "Value": "20191031"
//                      }
//                      ]
//                      }
//"""
//
//    //val newSupplierEvent1 = SupplierLoadedEvent(Math.random().toString, "Test", Option("Test2"), Option("Test3"))
//    val mockedEmail = mock[EmailAlerts]
//    val calendarInfo = mock[ProdCalendarInfo]
//    sparkExecutor.kafka.sendEventToKafka(newEvent, "1st test kafka message", topicName)
//    when(mockedService.appContext).thenReturn(appContext)
//    when(mockedService.kafkaOffsetPersistance).thenReturn(new KafkaOffsetPersistance(appContext, sparkExecutor.sql, new MDataKafkaOffset(appContext)))
//    when(mockedService.emailAlerts).thenReturn(mockedEmail)
//    when(mockedService.sparkExecutor).thenReturn(sparkExecutor)
//    when(mockedService.getkafkaTopicName).thenCallRealMethod()
//    when(mockedService.readEvent(any[Long])).thenCallRealMethod()
//    when(mockedService.addEventsToAccumulator(any[(DcpSupplierReadyEvent, String, Int, Long)])).thenCallRealMethod()
//    when(mockedService.processAccumulator(any[SupplierRecordsFile])).thenCallRealMethod()
//    when(mockedService.actionOnEventFailed(any[Exception])).thenAnswer(
//      new Answer[Unit] {
//        override def answer(invocation: InvocationOnMock): Unit = {
//          val e = invocation.getArguments()(0).asInstanceOf[Exception]
//          appContext.logger.logError(e)
//          appContext.logger.finishPipeline(eventOutcomeVal = appContext.logger.statusFailure, eventDescriptionVal = s"Step finished with error ${e.getMessage}")
//          failureCount = failureCount + 1
//        }
//      }    )
//    when(mockedService.main(any[Array[String]])).thenCallRealMethod()
//  }
//
//  it should "await and process events fine" ignore {
//  //ignore should "await and process events fine" in {
//    mockedService.main(
//      Array(
//        "-o", "0"
//      )
//    )
//    val dsuppDspnsdRxHdr = new DsuppDspnsdRxHdr(appContext)
//    val dsuppDspnsdRxDetl = new DsuppDspnsdRxDetl(appContext)
//    val dspnsdRxTransAcq = new DspnsdRxTransAcq(appContext)
//    val pbsFactOdsError = new PbsFactOdsErrLog(appContext)
//
//    println("ODS Header Rec Count is " + spark.table(dsuppDspnsdRxHdr.tableName).count())
//    //spark.table(dsuppDspnsdRxHdr.tableName).show(5, false)
//    assert(spark.table(dsuppDspnsdRxHdr.tableName).count() == 25)
//
//    println("ODS Stage table Rec Count is " + spark.table(dsuppDspnsdRxDetl.tableName).count())
//    //spark.table(dsuppDspnsdRxDetl.tableName).show(8, false)
//    assert(spark.table(dsuppDspnsdRxDetl.tableName).count() == 7924)
//
//    println("ODS fact table Rec Count is " + spark.table(dspnsdRxTransAcq.tableName).count())
//    spark.table(dspnsdRxTransAcq.tableName).show(8, false)
//    assert(spark.table(dspnsdRxTransAcq.tableName).count() == 7924)
//
//    println("ODS Error Rec Count is " + spark.table(pbsFactOdsError.tableName).count())
//    spark.table(pbsFactOdsError.tableName).show(9, false)
//
//    assert(failureCount == 0)
//  }
//}