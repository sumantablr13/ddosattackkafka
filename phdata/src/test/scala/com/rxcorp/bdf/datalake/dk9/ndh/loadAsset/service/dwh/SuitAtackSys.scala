package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util.UUID

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{KafkaOffsetPersistance, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.{SparkExecutor, TestSparkExecutor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark.NdhTransSum
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical.OdsLoadedEvent
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.spark.NdhTransSumAcq
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.EmailAlerts
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test.NdhUnitTest
import org.apache.spark.sql.SparkSession
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer


class SuitAtackSys extends NdhUnitTest {

  // test starts
  behavior of "ProcessDwhStream"
  lazy val stage = "ProcessDwhStream test"
  @transient private var sparkExecutor: SparkExecutor = _
  @transient private var spark: SparkSession = _
  @transient private var TopicName: String = "Sumanta"
  @transient private var failureCount: Long = 0

  val mockedService = mock[TestProcesAtackSys]


  override def beforeAll() {
    super.beforeAll()
    sparkExecutor = appContext.getSparkExecutor(appContext.config)
    spark = sparkExecutor.asInstanceOf[TestSparkExecutor].getSparkSession()

    case class abc(id:Int,ls:List[String])
    spark.sparkContext.parallelize(Seq(new abc(1,List("A","B"))))
    // prepare Kafka
    val kafkaTable = new MDataKafkaOffset(appContext)
     sparkExecutor.sql.createTable(kafkaTable)
    TopicName = appContext.config.getString("KAFKA_TOPIC_DWH_INPUT")
    kafka.createTopic(TopicName, 1)
    println(s"kafka topic created: ${TopicName}")

    // create SFTP paths
    val srcDir = appContext.config.getString("SFTP_SRCDIR") + "/" + ProcessDwhStreamConstants.BDC_DIR_NAME_APPROVED
    val srcDirTest = appContext.config.getString("SFTP_SRCDIR") + "/" + ProcessDwhStreamConstants.BDC_DIR_NAME_TEST
    new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, srcDir).mkdirs()
    new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, srcDirTest).mkdirs()

    val tgtDir = appContext.config.getString("SFTP_TGTDIR")
    new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, tgtDir).mkdirs()

    // create test file

    // empty files
    val srcFile_2 = new File("C:\\Users\\Sumanta.Banik\\Desktop\\SW\\NEMESA\\ndho\\170\\com.rxcorp.ndh\\datalake-dk9-com.rxcorp.ndh-loadAsset\\src\\test\\resources\\data\\dwh\\outputs\\tbl_company.csv")
    val srcFileTest_2 = new File("C:\\Users\\Sumanta.Banik\\Desktop\\SW\\NEMESA\\ndho\\170\\com.rxcorp.ndh\\datalake-dk9-com.rxcorp.ndh-loadAsset\\src\\test\\resources\\data\\dwh\\outputs\\tbl_company.csv")
    val tgtfile_2 = new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, s"$srcDir/gb9_bdc_pbs_rptbl_rec_ind_2_approval.csv")
    val tgtfileTest_2 = new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, s"$srcDirTest/gb9_bdc_pbs_rptbl_rec_ind_2_approval.csv")
    Files.copy(srcFile_2.toPath, tgtfile_2.toPath, StandardCopyOption.REPLACE_EXISTING)
    Files.copy(srcFileTest_2.toPath, tgtfileTest_2.toPath, StandardCopyOption.REPLACE_EXISTING)
    println("Execute")


    // prepare mocks
    when(mockedService.appContext).thenReturn(appContext)
    when(mockedService.sparkExecutor).thenReturn(sparkExecutor)
    when(mockedService.kafkaTable).thenReturn(kafkaTable)

    when(mockedService.kafkaOffsetPersistance).thenReturn(new KafkaOffsetPersistance(appContext, sparkExecutor.sql, kafkaTable))

    when(mockedService.main(any[Array[String]])).thenCallRealMethod()
  }

  it should "process 1st & 2nd event one-by-one fine" in {


    mockedService.main(
      Array(
      )
    )
  }
}



