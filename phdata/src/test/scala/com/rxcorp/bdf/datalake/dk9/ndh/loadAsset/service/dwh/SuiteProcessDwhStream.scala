package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh

import java.util.UUID

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.{SparkExecutor, TestSparkExecutor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{KafkaOffsetPersistance, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical.OdsLoadedEvent
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark.NdhTransSumAcq
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.EmailAlerts
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test.NdhUnitTest
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Encoders, SparkSession}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.io.Source

/**
  * Unit test for ProcessDwhStream class
  */
class SuiteProcessDwhStream extends NdhUnitTest {

  // test starts
  behavior of "ProcessDwhStream"
  lazy val stage = "ProcessDwhStream test"
  @transient private var sparkExecutor: SparkExecutor = _
  @transient private var spark: SparkSession = _
  @transient private var odsTopicName: String = _
  @transient private var odsTable: NdhTransSumAcq = _
  @transient private var dwhTable: NdhTransSum = _
  @transient private var failureCount: Long = 0

  val mockedService = mock[TestProcessDwhStream]
  val uuid1 = UUID.randomUUID().toString.toUpperCase()
  val uuid2 = UUID.randomUUID().toString.toUpperCase()

  override def beforeAll() {
    super.beforeAll()
    sparkExecutor = appContext.getSparkExecutor(appContext.config)
    spark = sparkExecutor.asInstanceOf[TestSparkExecutor].getSparkSession()

    // prepare Kafka
    val kafkaTable = new MDataKafkaOffset(appContext)
    sparkExecutor.sql.createTable(kafkaTable)
    odsTopicName = appContext.config.getString("KAFKA_TOPIC_DWH_INPUT")
    kafka.createTopic(odsTopicName, 1)
    println(s"kafka topic created: ${odsTopicName}")

    // prepare mdata tables
    val MDataTblLoadLog_ods = sparkExecutor.sql.getMetadataTableLoadLog("rmp")
    val MDataTblLoadLog_asset = sparkExecutor.sql.getMetadataTableLoadLog("rmp")
    sparkExecutor.sql.dropTable(MDataTblLoadLog_asset)
    sparkExecutor.sql.createTable(MDataTblLoadLog_asset)
    sparkExecutor.sql.dropTable(MDataTblLoadLog_ods)
    sparkExecutor.sql.createTable(MDataTblLoadLog_ods)


    // create ODS table
    odsTable = new NdhTransSumAcq(appContext)
    sparkExecutor.sql.dropTable(odsTable)
    sparkExecutor.sql.createTable(odsTable)

    //create DWH Table

    dwhTable = new NdhTransSum(appContext)
    sparkExecutor.sql.dropTable(dwhTable)
    sparkExecutor.sql.createTable(dwhTable)

    // load first file
    val inputStream = this.getClass.getResourceAsStream("/data/ods/outputs/dk9_so_trans_sum_acq_existing.csv")
    val lines = Source.fromInputStream(inputStream).getLines.toSeq
    val linesDS = spark.createDataset[String](lines)(Encoders.STRING)
    val tableDf = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("mode", "FAILFAST")
      .option("nullValue", "null")
      .csv(linesDS)
      .drop("src_sys_btch_id")
      .withColumn("src_sys_btch_id",lit(uuid1))
    tableDf.show(3,false)
    if (tableDf.count() == 0)
      throw new Exception("ERROR: Loaded 0 records from /data/ods/outputs/dk9_so_trans_sum_acq_existing.csv")
    sparkExecutor.sql.insertOverwriteTable(odsTable, tableDf, "Mocked existing source data for devl_dk9_ods.dk9_so_trans_sum_acq", "SB", "", new TimestampConverter())
    spark.sql(s"SELECT * FROM ${odsTable.tableName} LIMIT 10").show(10, false)

    // load second file
    val inputStream2 = this.getClass.getResourceAsStream("/data/ods/outputs/dk9_so_trans_sum_acq_ods.csv")
    val lines2 = Source.fromInputStream(inputStream2).getLines.toSeq
    val linesDS2 = spark.createDataset[String](lines2)(Encoders.STRING)
    val tableDf2 = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("mode", "FAILFAST")
      .option("nullValue", "null")
      .csv(linesDS2)
      .drop("src_sys_btch_id")
      .withColumn("src_sys_btch_id",lit(uuid2))
    if (tableDf2.count() == 0)
      throw new Exception("ERROR: Loaded 0 records from /data/ods/outputs/dk9_so_trans_sum_acq.csv")
    sparkExecutor.sql.insertTable(odsTable, tableDf2, "Mocked new source data for devl_dk9_ods.dk9_so_trans_sum_acq", "SB", "", new TimestampConverter())
    spark.sql(s"SELECT * FROM ${odsTable.tableName} LIMIT 5").show(10, false)

    // prepare mocks
    val mockedEmail = mock[EmailAlerts]
    when(mockedService.appContext).thenReturn(appContext)
    when(mockedService.sparkExecutor).thenReturn(sparkExecutor)
    when(mockedService.kafkaTable).thenReturn(kafkaTable)
    when(mockedService.emailAlerts).thenReturn(mockedEmail)
    when(mockedService.kafkaOffsetPersistance).thenReturn(new KafkaOffsetPersistance(appContext, sparkExecutor.sql, kafkaTable))
    when(mockedService.readEvent(any[Long])).thenCallRealMethod()
    when(mockedService.processEvents(any[(OdsLoadedEvent, String, Int, Long)])).thenCallRealMethod()
    when(mockedService.actionOnEventFailed(any[Exception],any[String], any[Int], any[Int],any[OdsLoadedEvent])).thenAnswer(
      new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val e = invocation.getArguments()(0).asInstanceOf[Exception]
         // appContext.logger.logError(e)
         // appContext.logger.finishPipeline(eventOutcomeVal = appContext.logger.statusFailure,eventDescriptionVal = s"Step finished with error ${e.getMessage}")
          failureCount = failureCount + 1
        }
      }
    )
    when(mockedService.main(any[Array[String]])).thenCallRealMethod()
  }
  it should "process 1st & 2nd event one-by-one fine" in {
    // add both events (check if code picks one only)
    val tblTs1 = TimestampConverter.parse("2020-09-28T14:26:42.577", "yyyy-MM-dd'T'HH:mm:ss.SSS")
    val topicTs1 = TimestampConverter.parse("2020-09-28T14:28:00.0", "yyyy-MM-dd'T'HH:mm:ss.SSS")

    val odsEvent1 = s"""{
                            "src_impl_pit_id":1,
                            "src_proc_pit_id":"SOURCE",
                            "data_typ_shrt_nm":"pbs_si_trans_acq_existing.csv",
                            "impl_btch_pit_id":"${uuid1}",
                            "supld_dsupp_proc_id":"A00049",
                            "dervd_dsupp_file_freq_typ_cd":"MONTHLY",
                            "dervd_dsupp_file_tm_pd_nbr":20190628,
                            "ops_tbl_isrted_ts":${tblTs1.millis.toString},
                            "ops_topic_isrted_ts":${topicTs1.millis.toString}

                   }""".stripMargin


    val tblTs2 = TimestampConverter.parse("2019-09-29T21:26:42.577", "yyyy-MM-dd'T'HH:mm:ss.SSS")
    val topicTs2 = TimestampConverter.parse("2019-09-29T21:46:00.0", "yyyy-MM-dd'T'HH:mm:ss.SSS")

    val odsEvent2 = s"""{
                            "src_impl_pit_id":1,
                            "src_proc_pit_id":"SOURCE",
                            "data_typ_shrt_nm":"pbs_si_trans_acq_ods.csv",
                            "impl_btch_pit_id":"${uuid2}",
                            "supld_dsupp_proc_id":"SST",
                            "dervd_dsupp_file_freq_typ_cd":"MONTHLY",
                            "dervd_dsupp_file_tm_pd_nbr":20190628,
                            "ops_tbl_isrted_ts":${tblTs2.millis.toString},
                            "ops_topic_isrted_ts":${topicTs2.millis.toString}
                   }""".stripMargin


    sparkExecutor.kafka.sendEventToKafka(odsEvent1, "ODS test kafka message no 1", odsTopicName)
    sparkExecutor.kafka.sendEventToKafka(odsEvent2, "ODS test kafka message no 2", odsTopicName)

    mockedService.main(
      Array(
      "-o", "0"
      )
    )

    val ndhTransSum = new NdhTransSum(appContext)

    assert(spark.table(ndhTransSum.tableName).count() ==20)
    spark.table(ndhTransSum.tableName).show(false)

    assert(spark.table(ndhTransSum.tableName).filter("current_timestamp() BETWEEN proc_eff_ts AND proc_expry_ts").count() == 14) //2 from ods + 2 from dwh
    assert(failureCount == 0)
  }

  it should  "process no events fine" in {
    mockedService.main(Array())
    assert(failureCount == 0)
  }

}
