package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh


import java.io.File

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{KafkaOffsetPersistance, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.{SparkExecutor, TestSparkExecutor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.{DdosAttackHys, DdosAttackHysT, MDataKafkaOffset}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test.NdhUnitTest
import org.apache.spark.sql.{Encoders, SparkSession}
import org.mockito.Matchers.any
import org.mockito.Mockito.when

import scala.io.Source



class SuitAtackSys extends NdhUnitTest {

  // test starts
  behavior of "ProcessDdosAttack"
  lazy val stage = "ProcessDdosAttack test"
  @transient private var sparkExecutor: SparkExecutor = _
  @transient private var spark: SparkSession = _
  @transient private var TopicName: String = "Sumanta"
  @transient private var failureCount: Long = 0

  val mockedService = mock[TestProcesAtackSys]


  override def beforeAll() {
    super.beforeAll()
    sparkExecutor = appContext.getSparkExecutor(appContext.config)
    spark = sparkExecutor.asInstanceOf[TestSparkExecutor].getSparkSession()


    // prepare Kafka
    val kafkaTable = new MDataKafkaOffset(appContext)
     sparkExecutor.sql.createTable(kafkaTable)

    val ddAttackHys = new DdosAttackHys(appContext)
    sparkExecutor.sql.createTable(ddAttackHys)


    TopicName = appContext.config.getString("KAFKA_TOPIC_PH_DATA")
    kafka.createTopic(TopicName, 1)
    println(s"kafka topic created: ${TopicName}")

    loadDataFromFile("/hysdata_ddos.csv", ",", ddAttackHys)
    // create test file

    def loadDataFromFile(filePath: String, delimiter: String, table: SparkTable): Unit = {
      val res = this.getClass.getResourceAsStream(filePath)
      val lines = Source.fromInputStream(res).getLines.toSeq
      val ds = spark.createDataset[String](lines)(Encoders.STRING)
      val df = spark.read
        .option("header", value = true)
        .option("delimiter", delimiter)
        .option("mode", "FAILFAST")
        .option("nullValue", "null")
        .csv(ds)

      if (df.count() == 0)
        throw new Exception(s"ERROR: Loaded 0 records from $filePath")
      sparkExecutor.sql.insertTable(table, df, s"Mocked existing source data for $filePath", "PH", "", new TimestampConverter())
    }


    // prepare mocks
    when(mockedService.appContext).thenReturn(appContext)
    when(mockedService.sparkExecutor).thenReturn(sparkExecutor)
    when(mockedService.kafkaTable).thenReturn(kafkaTable)

    when(mockedService.kafkaOffsetPersistance).thenReturn(new KafkaOffsetPersistance(appContext, sparkExecutor.sql, kafkaTable))

    when(mockedService.main(any[Array[String]])).thenCallRealMethod()
  }

  it should "load apache log and get the DDOS attack Ip address" in {


    mockedService.main(
      Array(
      )
    )

    val ddAttackHys = new DdosAttackHys(appContext)
    println(" Load the log file into the hys table")
    spark.table(ddAttackHys.tableName).show(200, false)
   println("Total log hystory load::"+ spark.table(ddAttackHys.tableName).count())
    assert(spark.table(ddAttackHys.tableName).count() > 2)
  }
}



