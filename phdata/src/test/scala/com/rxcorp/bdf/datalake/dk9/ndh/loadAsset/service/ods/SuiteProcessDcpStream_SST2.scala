package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.ods

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{KafkaOffsetPersistance, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.{SparkExecutor, TestSparkExecutor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.prodcalendar.ProdCalendarInfo
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.DAQELoadedEvent
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.physical.spark.{DaqeSSTTbl2}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.SupplierRecordsFile
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.{EmailAlerts, FileContentValidation, HeaderValidation, ValidationRunner}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test.NdhUnitTest
import org.apache.spark.sql._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.apache.spark.sql.functions.lit

import scala.io.Source

/**
  * Unit test for ProcessDcpStream class
  */
class SuiteProcessDcpStream_SST2 extends NdhUnitTest {

  // test starts
  behavior of "ProcessOdsDcpStream"
  lazy val stage = "ProcessOdsDcpStream test"
  @transient private var sparkExecutor: SparkExecutor = _
  @transient private var spark: SparkSession = _
  @transient private var failureCount: Long = 0

  val mockedService = mock[TestProcessDcpStream]
  val uuid1 = s"${java.util.UUID.randomUUID()}".split('-').map(_.trim).mkString
  var newEvent : String =""

  override def beforeAll() {
    super.beforeAll()
    val defaultFileContentValidations = Seq(
      //FileContentValidation.NumericDataTypeValidation(appContext),
      //      FileContentValidation.BlankTransDateValidation(appContext)
      //            FileContentValidation.NumericDataTypeValidation(appContext),
      // FileContentValidation.DecimalDataTypeValidation(appContext),
      //FileContentValidation.DecimalDataTypeWithCommaValidation(appContext),
      //FileContentValidation.ZeroValueValidation(appContext),
      //      //FileContentValidation.QuantityNumericDataTypeValidation(appContext),
      //FileContentValidation.NegativeValueValidation(appContext),
      //FileContentValidation.EmptyValueValidation(appContext)
      //      FileContentValidation.DayNbrValidation(appContext),
      //      FileContentValidation.MonthNbrValidation(appContext),
      //      FileContentValidation.YearNbrValidation(appContext),
      //      FileContentValidation.BlankClientCodeValidation(appContext),
      //      FileContentValidation.QuantityDecimalDataTypeValidation(appContext)
      //      FileContentValidation.BlankTransDateValidation(appContext)
    )


    val defaultHeaderValidations = Seq(
      //      HeaderValidation.DayNbrValidation(appContext),
      //      HeaderValidation.WeekNbrValidation(appContext),
      //      HeaderValidation.MonthNbrValidation(appContext),
      //      HeaderValidation.YearNbrValidation(appContext)
      // HeaderValidation.NumericDataTypeValidation(appContext)
    )

    val defaultValidationsRunner = new ValidationRunner(defaultFileContentValidations, defaultHeaderValidations)
    when(mockedService.defaultValidationsRunner).thenReturn(defaultValidationsRunner)
    sparkExecutor = appContext.getSparkExecutor(appContext.config)

    val mDataTblLoadLog = sparkExecutor.sql.getMetadataTableLoadLog("SB")
    sparkExecutor.sql.dropTable(mDataTblLoadLog)
    sparkExecutor.sql.createTable(mDataTblLoadLog)

    val kafkaTable = new MDataKafkaOffset(appContext)
    sparkExecutor.sql.dropTable(kafkaTable)
    sparkExecutor.sql.createTable(kafkaTable)

    spark = sparkExecutor.asInstanceOf[TestSparkExecutor].getSparkSession()

    //SST dump table
    val sstDaqe = new DaqeSSTTbl2(appContext)
    sparkExecutor.sql.dropTable(sstDaqe)
    sparkExecutor.sql.createTable(sstDaqe)

    val sstDaqeInfo= this.getClass.getResourceAsStream("/data/daqe/SST/SSTTbl2.csv")
    val sstDaqeLines = Source.fromInputStream(sstDaqeInfo).getLines.toSeq
    val sstDaqeDS = spark.createDataset[String](sstDaqeLines)(Encoders.STRING)
    sstDaqeDS.show(false)

    val sstDaqeDf = spark.read
      .option("header", value = true)
      .option("delimiter", "|")
      .option("mode", "FAILFAST")
      .option("nullValue", "null")
      .csv(sstDaqeDS)
      .drop("runid")
        .withColumn("runid", lit(uuid1))
    sstDaqeDf.show(5, truncate = false)

    if (sstDaqeDf.count() == 0)
      throw new Exception("ERROR: Loaded 0 records from /data/daqe/SST/SSTTbl2.csv")
    sparkExecutor.sql.insertOverwriteTable(sstDaqe, sstDaqeDf, "Mocked existing source data for DAQE dk9_sst_tbl_2_load table", "SB", "", new TimestampConverter())
    spark.sql(s"SELECT * FROM ${sstDaqe.tableName} LIMIT 10").show(2, truncate = false)

//    //postgres table data
//    val fileMDataInfo = this.getClass.getResourceAsStream("/data/daqe/mdata/mData.csv")
//    val mDataLines = Source.fromInputStream(fileMDataInfo).getLines.toSeq
//    val mdataDS = spark.createDataset[String](mDataLines)(Encoders.STRING)
//    mdataDS.show(false)
//
//    val mDataDaqeDF = spark.read
//      .option("header", value = true)
//      .option("delimiter", "|")
//      .option("mode", "FAILFAST")
//      .option("nullValue", "null")
//      .csv(mdataDS)
//    mDataDaqeDF.show(false)
//
//    if (mDataDaqeDF.count() == 0)
//      throw new Exception("ERROR: Loaded 0 records from /data/daqe/SST/SSTTbl2.csv")
    /*sparkExecutor.sql.insertOverwriteTable(sstDaqe, sstDaqeDf, "Mocked existing source data for DAQE dk9_sst_tbl_2_load table", appContext.envApplicationCode, "", new TimestampConverter())
    spark.sql(s"SELECT * FROM ${sstDaqe.tableName} LIMIT 10").show(2, truncate = false)*/


    // Creating Error Log Table
    val ndhFactOdsError = new NdhFactOdsErrLog(appContext)
    sparkExecutor.sql.dropTable(ndhFactOdsError)
    sparkExecutor.sql.createTable(ndhFactOdsError)

    // Creating ODS Fact Table
    val ndhFactOds = new NdhTransSumAcq(appContext)
    sparkExecutor.sql.dropTable(ndhFactOds)
    sparkExecutor.sql.createTable(ndhFactOds)

   // spark.read.format("csv").load("src/test/resources/data/dcp/kf.pwd").write.format("csv").option("header", "false").mode(SaveMode.Overwrite).save(s"${appContext.config.getString("HDFS_USER_PATH")}/${appContext.config.getString("KF_PWD")}")
    kafka.createTopic(appContext.config.getString("KAFKA_TOPIC_DWH_INPUT"))
    kafka.createTopic(appContext.config.getString("KAFKA_TOPIC_DAQE_SST"))
    val topicName = appContext.config.getString("KAFKA_TOPIC_DAQE_SST")
    newEvent =
      s"""
         |{
         |"action": "Denmark_SST_SO_Load_DataValidated",
         |  "eventType": "QC0 Validations",
         |  "payload": {
         |    "batchTable": "devl_dk9_ods.dk9_sst_tbl_2_load",
         |    "client": "Denmark",
         |    "kafkaPiggyBack": "{}",
         |    "layoutName": "SST_SO_Load",
         |    "runId": "$uuid1",
         |    "tabToValidate": "dk9_sst_tbl_2_load",
         |    "targetSchema": "devl_dk9_ods"
         |  },
         |  "status": "Success",
         |  "ts": 1599753351357
         |}
       """.stripMargin

    //val newSupplierEvent1 = SupplierLoadedEvent(Math.random().toString, "Test", Option("Test2"), Option("Test3"))
    val mockedEmail = mock[EmailAlerts]
    val calendarInfo = mock[ProdCalendarInfo]
    sparkExecutor.kafka.sendEventToKafka(newEvent, "1st test kafka message", topicName)
    when(mockedService.appContext).thenReturn(appContext)
    when(mockedService.kafkaOffsetPersistance).thenReturn(new KafkaOffsetPersistance(appContext, sparkExecutor.sql, new MDataKafkaOffset(appContext)))
    when(mockedService.emailAlerts).thenReturn(mockedEmail)
    when(mockedService.sparkExecutor).thenReturn(sparkExecutor)
    when(mockedService.getkafkaTopicName()).thenCallRealMethod()
    when(mockedService.readEvent(any[Long], any[String])).thenCallRealMethod()
    when(mockedService.addEventsToAccumulator(any[(DAQELoadedEvent, String, Int, Long)])).thenCallRealMethod()
    when(mockedService.processAccumulator(any[SupplierRecordsFile])).thenCallRealMethod()
    when(mockedService.actionOnEventFailed(any[Exception])).thenAnswer(
      new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val e = invocation.getArguments()(0).asInstanceOf[Exception]
         // appContext.logger.logError(e)
         // appContext.logger.finishPipeline(eventOutcomeVal = appContext.logger.statusFailure, eventDescriptionVal = s"Step finished with error ${e.getMessage}")
          failureCount = failureCount + 1
        }
      } )
    when(mockedService.main(any[Array[String]])).thenCallRealMethod()
  }

  it should "await and process events fine" ignore  {
  //ignore should "await and process events fine" in {
    mockedService.main(
      Array(
        "-o", "0",
        "-t", appContext.config.getString("KAFKA_TOPIC_DAQE_SST")
      )
    )

    val transSumAcq = new NdhTransSumAcq(appContext)
    val ndhFactOdsError = new NdhFactOdsErrLog(appContext)

    println("ODS table Rec Count is " + spark.table(transSumAcq.tableName).count())
    spark.table(transSumAcq.tableName).show(20, false)
    assert(spark.table(transSumAcq.tableName).count() == 10)

    println("ODS Error Rec Count is " + spark.table(ndhFactOdsError.tableName).count())
    spark.table(ndhFactOdsError.tableName).show(20, false)

    assert(failureCount == 0)
  }
}
