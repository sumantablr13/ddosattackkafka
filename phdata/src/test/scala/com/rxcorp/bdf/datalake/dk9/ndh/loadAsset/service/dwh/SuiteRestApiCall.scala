
package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.{SparkExecutor, TestSparkExecutor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test.NdhUnitTest
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import com.github.tomakehurst.wiremock.stubbing.StubMapping

import scala.io.Source
/**
  * Unit test for restApiCall class
  */
class SuiteRestApiCall extends NdhUnitTest {

  // test starts
  behavior of "RestApiCall"
  //lazy val stage = "ProductionCycleManager test"
  @transient private var sparkExecutor: SparkExecutor = _
  @transient private var spark: SparkSession = _
  @transient private var failureCount: Long = 0

  val mockedService = mock[TestRestApiCallExamples]

  override def beforeAll() {
    super.beforeAll()

    sparkExecutor = appContext.getSparkExecutor(appContext.config)
    spark = sparkExecutor.asInstanceOf[TestSparkExecutor].getSparkSession()

    // put bdl.pwd on HDFS
    spark.read.format("csv").load("src/test/resources/bdl.pwd")
      .write.format("csv").option("header", "false").save(s"${appContext.config.getString("HDFS_USER_PATH")}/${appContext.config.getString("REST_API_PASSWORD")}")

    //Mock http URI
    val httpPostJson: String = Source.fromFile("src/test/resources/data/restApi/httpPostLdap.json").getLines.mkString
    val httpGetJson: String = Source.fromFile("src/test/resources/data/restApi/httpGetData.json").getLines.mkString
    restHttpServer.addStubMapping(StubMapping.buildFrom(httpPostJson))
    restHttpServer.addStubMapping(StubMapping.buildFrom(httpGetJson))

    // prepare mocks
    when(mockedService.appContext).thenReturn(appContext)
    when(mockedService.sparkExecutor).thenReturn(sparkExecutor)
    when(mockedService.processOptions()).thenCallRealMethod()
    when(mockedService.actionOnEventFailed(any[Exception])).thenAnswer(
      new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val e = invocation.getArguments()(0).asInstanceOf[Exception]
         // appContext.logger.error(e.toString() + " --> " + ExceptionUtils.getStackTrace(e))
         // appContext.logger.finishPipeline(eventOutcomeVal = appContext.logger.statusFailure)
          failureCount = failureCount + 1
        }
      }
    )
    when(mockedService.main(any[Array[String]])).thenCallRealMethod()
  }

  it should "Make Rest Api Call" in {
    failureCount = 0
    mockedService.main(Array())
    assert(failureCount == 0)
  }

 }
