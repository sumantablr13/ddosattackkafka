package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import java.io.File
import java.util.{Date, Properties}

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class TestAppContext(kafkaServer: String, tableSuffix: String, tempDir: File) extends AppContextTrait {

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.ERROR)

  val dk9Database = "ph_data"


  val localMetastorePath = new File(tempDir, "metastore").getCanonicalPath
  val localWarehousePath = { val f = new File(tempDir, "warehouse"); f.mkdirs(); f.getCanonicalPath }
  val hdfsDir = { val f = new File(tempDir, "hdfs"); f.mkdirs(); f.getCanonicalPath }
  val sftpDir = { val f = new File(tempDir, "sftp"); f.mkdirs(); f.getCanonicalPath }
  val restServerDir = { val f = new File(tempDir, "restapi"); f.mkdirs(); f.getCanonicalPath }

  override def getSparkSession(): SparkSession = {
    val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
      .set("datanucleus.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
      .set(ConfVars.METASTOREURIS.varname, "")
      .set("spark.sql.streaming.checkpointLocation", tempDir.toPath().toString)
      .set("spark.sql.warehouse.dir", localWarehousePath)
    _spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    _spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString)
    return _spark
  }

  override def getSparkExecutor(config: Config): SparkExecutor = {
   _spark.sql(s"create database if not exists ${dk9Database}")
    setupSparkSession( _spark, dk9Database)
    new TestSparkExecutor( _spark, kafkaServer)
  }



  override def getSftpExecutor(sftpHost:String,sftpUser:String,sftpPassword:String,sftpPortNo:Int): SftpExecutor = {
    if (_sftpExecutor != null) {
      _sftpExecutor.closeConnection()
      _sftpExecutor = null
    }
    return new TestSftpExecutor( sftpHost, sftpUser, sftpPassword, sftpPortNo)
  }


  override def getConfig(): Config = {
    val prop = new Properties()


    prop.setProperty("dk9_DATABASE", dk9Database)

    prop.setProperty("dk9_DATABASE_BATCH", dk9Database)
    prop.setProperty("HDFS_NAME_NODE", hdfsDir)
    prop.setProperty("HDFS_USER_BASE", s"/user")
    prop.setProperty("HDFS_USER_PATH", s"${hdfsDir}/user/dk9dusr")

    prop.setProperty("KAFKA_TOPIC_PH_DATA", "global.phd.data.block.ipaddress")
    prop.setProperty("KAFKA_DDOS_PROCESS_NAME", "ddos_attack_kafka")
    prop.setProperty("APPACHE_LOG_LOC", "C:\\Users\\Sumanta.Banik\\Desktop\\SW\\NEMESA\\ndho\\phcode\\ddosattackkafka\\phddosattack\\src\\test\\resources\\")
    prop.setProperty("DDOS_FILE_LOC", "C:\\Users\\Sumanta.Banik\\Desktop\\SW\\NEMESA\\ndho\\phcode\\ddosattackkafka\\phddosattack\\src\\test\\resources\\ddos_ipaddress.txt")
    prop.setProperty("INERVEL_TIME", "8")
    prop.setProperty("SLICE_PER_TOPIC", "20")


    prop.setProperty("KAFKA_SERVER_DAQE" , "localhost:49311")


    //Mock sftp
    prop.setProperty("SFTP_HOST","localhost")
    prop.setProperty("SFTP_PORT","22000")
    prop.setProperty("SFTP_USER", "dk9TestUser")
    prop.setProperty("SFTP_PASSWORD", "moveit.pwd")
    prop.setProperty("SFTP_TGTDIR","/development/dk9/data/ndh")
    prop.setProperty("SFTP_SRCDIR", "development/dk9/data/ndh/sftp")


    ConfigFactory.parseProperties(prop)
  }
}
