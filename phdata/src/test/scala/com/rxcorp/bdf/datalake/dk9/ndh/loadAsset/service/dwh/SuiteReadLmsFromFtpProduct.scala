
package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.TimestampConverter
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.{SftpExecutor, SparkExecutor, TestSparkExecutor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.SparkTable
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.dwh.physical.spark._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.EmailAlerts
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test.NdhUnitTest
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.io.Source

/**
  * Unit test for SampleSftpTransfer class
  */

class SuiteReadLmsFromFtpProduct extends NdhUnitTest {

  // test starts
  behavior of "ReadLmsFromFtp"
  //lazy val stage = "ProductionCycleManager test"
  @transient private var sparkExecutor: SparkExecutor = _
  @transient private var spark: SparkSession = _
  @transient private var failureCount: Long = 0
  @transient private var nonCriticalCount: Long = 0
  @transient private var currentTs: TimestampConverter =_

  val mockedService: TestReadLmsFromFtp = mock[TestReadLmsFromFtp]


  override def beforeAll() {
    super.beforeAll()

    sparkExecutor = appContext.getSparkExecutor(appContext.config)
    spark = sparkExecutor.asInstanceOf[TestSparkExecutor].getSparkSession()

    // put sftp.pwd on HDFS
    spark.read.format("csv").load("src/test/resources/lmsftp.pwd")
      .write.format("csv").option("header", "false").save(s"${appContext.config.getString("HDFS_USER_PATH")}/${appContext.config.getString("LMS_SFTP_PASSWORD")}")

    // create SFTP paths
    val srcDir = appContext.config.getString("LMS_SFTP_SRCDIR" )+"/"+ "NYESTE"
    new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, srcDir).mkdirs()

//    val tgtDir = appContext.config.getString("SFTP_TGTDIR")
//    new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, tgtDir).mkdirs()

    // create test file

    // empty files
    new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, s"$srcDir/LMS_1.txt").createNewFile()

    // actual test file
    val srcFile=new File("src/test/resources/data/lms/lms01.txt")
    val tgtfile= new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, s"$srcDir/lms01.txt")
    Files.copy(srcFile.toPath,tgtfile.toPath,StandardCopyOption.REPLACE_EXISTING)

    val srcFilePack = new File("src/test/resources/data/lms/lms02.txt")
    val tgtfilePack = new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, s"$srcDir/lms02.txt")
    Files.copy(srcFilePack.toPath, tgtfilePack.toPath, StandardCopyOption.REPLACE_EXISTING)

    val srcFileCompany = new File("src/test/resources/data/lms/lms09.txt")
    val tgtfileCompany = new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, s"$srcDir/lms09.txt")
    Files.copy(srcFileCompany.toPath, tgtfileCompany.toPath, StandardCopyOption.REPLACE_EXISTING)

    val srcFileDrugNames = new File("src/test/resources/data/lms/lms21.txt")
    val tgtfileDrugNames = new File(sftpServer.getFileSystemFactory.asInstanceOf[VirtualFileSystemFactory].getDefaultHomeDir.toFile.getCanonicalPath, s"$srcDir/lms21.txt")
    Files.copy(srcFileDrugNames.toPath, tgtfileDrugNames.toPath, StandardCopyOption.REPLACE_EXISTING)

    // prepare mdata tables
    val mDataTblLoadLog_asset = sparkExecutor.sql.getMetadataTableLoadLog("RM")
    sparkExecutor.sql.dropTable(mDataTblLoadLog_asset)
    sparkExecutor.sql.createTable(mDataTblLoadLog_asset)

    // prepare product target table

    val mDataLmsProduct = new LmsProduct(appContext)
    sparkExecutor.sql.dropTable(mDataLmsProduct)
    sparkExecutor.sql.createTable(mDataLmsProduct)
    val productInfo= this.getClass.getResourceAsStream("/data/dwh/outputs/tbl_product.csv")
    val productLines = Source.fromInputStream(productInfo).getLines.toSeq
    val productDS = spark.createDataset[String](productLines)(Encoders.STRING)
    productDS.show(false)

    val productReadDf = spark.read
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "FAILFAST")
      .option("nullValue", "null")
      .csv(productDS)
    productReadDf.show(false)

//    if (productReadDf.count() == 0)
//      throw new Exception("ERROR: Loaded 0 records from /data/dwh/outputs/tbl_product.csv")
    sparkExecutor.sql.insertOverwriteTable(mDataLmsProduct, productReadDf, "Mocked existing source data for dwh.tbl_product table", "SB", "", new TimestampConverter())

    val mDataLmsPack = new LmsPack(appContext)
    sparkExecutor.sql.dropTable(mDataLmsPack)
    sparkExecutor.sql.createTable(mDataLmsPack)
    val packInfo = this.getClass.getResourceAsStream("/data/dwh/outputs/tbl_pack.csv")
    val packLines = Source.fromInputStream(packInfo).getLines.toSeq
    val packDS = spark.createDataset[String](packLines)(Encoders.STRING)
    packDS.show(false)

    val packReadDf = spark.read
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "FAILFAST")
      .option("nullValue", "null")
      .csv(packDS)
    packReadDf.show(false)

    //    if (productReadDf.count() == 0)
    //      throw new Exception("ERROR: Loaded 0 records from /data/dwh/outputs/tbl_product.csv")
    sparkExecutor.sql.insertOverwriteTable(mDataLmsPack, packReadDf, "Mocked existing source data for dwh.tbl_pack table", "SB", "", new TimestampConverter())

    val mDataLmsCompany = new LmsCompany(appContext)
    sparkExecutor.sql.dropTable(mDataLmsCompany)
    sparkExecutor.sql.createTable(mDataLmsCompany)
    val companyInfo = this.getClass.getResourceAsStream("/data/dwh/outputs/tbl_company.csv")
    val companyLines = Source.fromInputStream(companyInfo).getLines.toSeq
    val companyDS = spark.createDataset[String](companyLines)(Encoders.STRING)
    companyDS.show(false)

    val companyReadDf = spark.read
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "FAILFAST")
      .option("nullValue", "null")
      .csv(companyDS)
    companyReadDf.show(false)

    //    if (productReadDf.count() == 0)
    //      throw new Exception("ERROR: Loaded 0 records from /data/dwh/outputs/tbl_product.csv")
    sparkExecutor.sql.insertOverwriteTable(mDataLmsCompany, companyReadDf, "Mocked existing source data for dwh.tbl_company table", "SB", "", new TimestampConverter())

    val mDataLmsDrugName = new LmsDrugNames(appContext)
    sparkExecutor.sql.dropTable(mDataLmsDrugName)
    sparkExecutor.sql.createTable(mDataLmsDrugName)
    val drugNameInfo = this.getClass.getResourceAsStream("/data/dwh/outputs/tbl_drug_names.csv")
    val drugNameLines = Source.fromInputStream(drugNameInfo).getLines.toSeq
    val drugNameDS = spark.createDataset[String](drugNameLines)(Encoders.STRING)
    drugNameDS.show(false)

    val drugNameReadDf = spark.read
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "FAILFAST")
      .option("nullValue", "null")
      .csv(drugNameDS)
    drugNameReadDf.show(false)

    //    if (productReadDf.count() == 0)
    //      throw new Exception("ERROR: Loaded 0 records from /data/dwh/outputs/tbl_product.csv")
    sparkExecutor.sql.insertOverwriteTable(mDataLmsDrugName, drugNameReadDf, "Mocked existing source data for dwh.tbl_drug_names table", "SB", "", new TimestampConverter())

    // prepare mocks
    val mockedEmail = mock[EmailAlerts]

    when(mockedService.appContext).thenReturn(appContext)
    when(mockedService.sparkExecutor).thenReturn(sparkExecutor)
    when(mockedService.processOptions(any[ReadLmsFromFtpArgs])).thenCallRealMethod()
    when(mockedService.emailAlerts).thenReturn(mockedEmail)
    when(mockedService.actionOnEventFailed(any[Exception], anyString, anyString, any[TimestampConverter],anyString, anyBoolean)).thenAnswer(
      new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val args = invocation.getArguments
          val e = args(0).asInstanceOf[Exception]
          val correlationUUID = args(1).asInstanceOf[String]
          val procFileName = args(2).asInstanceOf[String]
          val fileCreTs = args(3).asInstanceOf[TimestampConverter]
          val procName = args(4).asInstanceOf[String]
          val shouldTerminate = args(5).asInstanceOf[Boolean]
          //appContext.logger.logError(e)
         // appContext.logger.finishPipeline(eventOutcomeVal = appContext.logger.statusFailure, eventDescriptionVal = s"Step finished with error ${e.getMessage}")
          mockedEmail.sendStatusEmail(s"LMS Product load process failed, ${e.toString}",currentTs.notTsUTCText("yyyyMMdd") ,procName)
          if (shouldTerminate)
            failureCount = failureCount + 1
          else
            nonCriticalCount = nonCriticalCount + 1
        }
      }
    )
    when(mockedService.main(any[Array[String]])).thenCallRealMethod()
  }

  it should "Copy LMS files from FTP to respective target table" in {
    failureCount = 0
    nonCriticalCount = 0
    mockedService.main(
      Array(
        "--lmsReadLocation", "NYESTE"
      )
    )

    assert(failureCount == 0 && nonCriticalCount == 0)
  }

}
