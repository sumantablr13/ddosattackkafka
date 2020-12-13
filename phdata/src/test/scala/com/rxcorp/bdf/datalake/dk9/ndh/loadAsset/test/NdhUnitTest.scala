package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test

import java.io.{File, IOException}
import java.nio.file.Paths
import java.util.{Collections, UUID}

import org.apache.sshd.server.SshServer
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, TestAppContext}
import com.rxcorp.bdf.logging.internal.StandardImsLogger
import info.batey.kafka.unit.KafkaUnit
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.Logger
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options




/** Shares a local `HiveContext` between all tests in a suite and closes it at the end. */
trait SharedAppContext extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _testAppContext: TestAppContext = _
  @transient private var _tempDir: File = _
  @transient var appContext: AppContext = _
  @transient var kafka: KafkaUnit = _
  @transient var sftpServer: SshServer = _
  @transient var restHttpServer: WireMockServer = _
  lazy val currentUser = "SumantaUser"
  lazy val uuid = "SuamntaUID"

  // 1.2. Instantiate new SparkContext and set logging level
  override def beforeAll() {
    kafka = new KafkaUnit(49310, 49311)
    val cleanerBufferSize: Long = 2 * 1024 * 1024L
    kafka.setKafkaBrokerConfig("log.cleaner.dedupe.buffer.size", cleanerBufferSize.toString)
    kafka.startup()
    println("Broker Port is " + kafka.getBrokerPort + " kafka connect is " + kafka.getKafkaConnect)
    _tempDir = new File("target", "spark-" + UUID.randomUUID.toString)
    _testAppContext = new TestAppContext(kafka.getKafkaConnect, s"${currentUser}_${uuid}", _tempDir)
    //   _testAppContext = new TestAppContext( "kafka-0-broker.dev-ukws-kafka01.cdt-dev.dcos:9612,kafka-1-broker.dev-ukws-kafka01.cdt-dev.dcos:9612,kafka-2-broker.dev-ukws-kafka01.cdt-dev.dcos:9612", s"${currentUser}_${uuid}")
    appContext = _testAppContext.apply()

    sftpServer = SshServer.setUpDefaultServer()
    sftpServer.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(Paths.get(s"${_testAppContext.sftpDir}/hostkey.ser")))
    sftpServer.setHost(_testAppContext.getConfig().getString("SFTP_HOST"))
    sftpServer.setPort(_testAppContext.getConfig().getInt("SFTP_PORT"))
    sftpServer.setPasswordAuthenticator(new PasswordAuthenticator {
      override def authenticate(s: String, p: String, serverSession: ServerSession): Boolean = {
        val user = _testAppContext.getConfig().getString("SFTP_USER")
        val password = "testme"
        if (s == user && p == password)
          return true
        else
          return false
      }
    })
    val factory = new SftpSubsystemFactory.Builder().build
    sftpServer.setSubsystemFactories(Collections.singletonList(factory))
    println(s"${_testAppContext.sftpDir}")
    sftpServer.setFileSystemFactory(new VirtualFileSystemFactory(Paths.get(s"${_testAppContext.sftpDir}")))
    sftpServer.start()



    super.beforeAll()
  }

  // 1.3. stop SparkContext
}

/**
  * Abstract NDH unit test.
  */
abstract class NdhUnitTest extends FlatSpec with Matchers with MockitoSugar with SharedAppContext
