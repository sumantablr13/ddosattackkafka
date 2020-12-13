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

  val dk9Database = "devl_dk9"
  val dk9DatabaseOds = "devl_dk9_ods"
  val dk9DatabaseRds = "devl_rds_dk"

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
      //.set("hive.exec.scratchdir","C:\\test\\tmp\\hive")
//      .set("hive.exec.scratchdir","C:\\Users\\gsubramani\\AppData\\Local\\tmp\\hive") //local setting
    _spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    _spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString)
    return _spark
  }

  override def getSparkExecutor(config: Config): SparkExecutor = {
   _spark.sql(s"create database if not exists ${dk9Database}")
    _spark.sql(s"create database if not exists ${dk9DatabaseOds}")
    _spark.sql(s"create database if not exists ${dk9DatabaseRds}")
    setupSparkSession( _spark, dk9Database)
    setupSparkSession( _spark, dk9DatabaseRds)
    new TestSparkExecutor( _spark, kafkaServer)
  }

 /* override def getImpalaExecutor(config: Config): ImpalaExecutor = {
    val impalaJdbcUrl =  config.getString("IMPALA_JDBC_URL").trim
    val impalaJdbcDriver =  config.getString("IMPALA_JDBC_DRIVER").trim
    val impalaMemoryLimit = config.getString("IMPALA_MEM_LIMIT").trim

    if (_impalaJdbcConnection == null)
    {
      //_impalaJdbcConnection = getImpalaConnection( impalaJdbcDriver, impalaJdbcUrl)
      if (_impalaJdbcConnection == null) return null
     // setupImpalaConnection(logger, _impalaJdbcConnection, impalaMemoryLimit)
    }
    return new TestImpalaExecutor(, _spark, _impalaJdbcConnection, tableSuffix)
  }*/

  override def getSftpExecutor(sftpHost:String,sftpUser:String,sftpPassword:String,sftpPortNo:Int): SftpExecutor = {
    if (_sftpExecutor != null) {
      _sftpExecutor.closeConnection()
      _sftpExecutor = null
    }
    return new TestSftpExecutor( sftpHost, sftpUser, sftpPassword, sftpPortNo)
  }

/*
  override def restfulServiceExecutor(): RestfulServiceExecutor = {
    if (_RestfulServiceExecutor != null) {
      _RestfulServiceExecutor = null
    }
    return new RestfulServiceExecutor()
  }
*/

  override def getConfig(): Config = {
    val prop = new Properties()
    prop.setProperty("ENV_APPLICATION_CODE", "ndh")
    prop.setProperty("ENV_DEFAULT_LOG_LEVEL", "INFO")
    prop.setProperty("ENV_LIFECYCLE_SHORT", "unit")
    prop.setProperty("ENV_LIFECYCLE_LONG", "unittest")
    prop.setProperty("ENV_TENANT_CODE", "dk9")
    prop.setProperty("ENV_TENANT_CODE_SHRT", "dk")
    prop.setProperty("ENV_TENANT_DESC", "Nordics Denmark")
    prop.setProperty("ENV_APPLICATION_DESC", "PRESCRIPTION DATA")
    prop.setProperty("ENV_USER", "dk9dusr")
    prop.setProperty("dk9_DATABASE", dk9Database)
    prop.setProperty("dk9_DATABASE_ODS", dk9DatabaseOds)
    prop.setProperty("dk9_DATABASE_RDS", dk9DatabaseRds)
    prop.setProperty("dk9_DATABASE_BATCH", dk9Database)
    prop.setProperty("HDFS_NAME_NODE", hdfsDir)
    prop.setProperty("HDFS_USER_BASE", s"/user")
    prop.setProperty("HDFS_USER_PATH", s"${hdfsDir}/user/dk9dusr")
    prop.setProperty("IMPALA_JDBC_DRIVER", "com.cloudera.impala.jdbc41.Driver")
    prop.setProperty("IMPALA_JDBC_URL", s"jdbc:impala://usdhdpimpala:21050/$dk9Database;AuthMech=3;SSL=1;SSLTrustStore=src/test/resources/jssecacerts;UID=dk9dusr;PWD=dk9devl1;REQUEST_POOL=dk9")
    // prop.setProperty("IMPALA_JDBC_URL", s"jdbc:impala://usdhdpimpala.rxcorp.com:21050/DEVL_DF3_dk9;AuthMech=3;SSL=1;SSLTrustStore=src/test/resources/jssecacerts;KrbHostFQDN=_HOST@INTERNAL.IMSGLOBAL.COM;KrbServiceName=impala;UID=dk9dusr;REQUEST_POOL=dk9;PWD=")
    prop.setProperty("IMPALA_MEM_LIMIT", "3G")
    prop.setProperty("KAFKA_TOPIC_SUPPLIER_READY", "unit.dk9.com.rxcorp.ndh.sales.supplierReady")
//    prop.setProperty("KAFKA_TOPIC_PCMDTY_UNBRIDGED", "unit.dk9.com.rxcorp.ndh.pcmdty.unbridged")
//    prop.setProperty("KAFKA_TOPIC_DISTING_OAC_UNBRIDGED", "unit.dk9.com.rxcorp.ndh.distingOac.unbridged")
//    prop.setProperty("KAFKA_TOPIC_DSPNSING_OAC_UNBRIDGED", "unit.dk9.com.rxcorp.ndh.dspnsingOac.unbridged")
    prop.setProperty("KAFKA_TOPIC_DWH_INPUT", "unit.dk9.com.rxcorp.ndh.sales.dwhInput")
    prop.setProperty("KAFKA_TOPIC_DWH_READY", "unit.dk9.com.rxcorp.ndh.sales.dwhReady")
//    prop.setProperty("KAFKA_TOPIC_PROJECTION_FACTOR_READY", "unit.dk9.com.rxcorp.ndh.sales.projectionFactorReady")
    prop.setProperty("KAFKA_TOPIC_DAQE_SST", "UNIT_Denmark_SST_SO_Load_DataValidated")
    prop.setProperty("KAFKA_TOPIC_DAQE_AMGROS", "UNIT_Denmark_Amgros_SI_Load_DataValidated")


    prop.setProperty("KAFKA_SERVER_DAQE" , "localhost:49311")
    prop.setProperty("LOG_CONSOLE_APPENDER", "true")
    prop.setProperty("LOG_ELASTIC_APPENDER", "true")
    prop.setProperty("LOG_ELASTIC_INDEX_NAME", "sandbox_test_write")
    prop.setProperty("LOG_ELASTIC_INDEX_NAME_QC", "xg_devl_bdf_com.rxcorp.bdf.qc_write")
    prop.setProperty("LOG_ELASTIC_TYPE", "composerJob")
    prop.setProperty("LOG_ELASTIC_URL", "http://bdfesdev.imshealth.com:9200/")
    prop.setProperty("LOG_FILE_APPENDER", "false")
    prop.setProperty("LOG_FILE_NAME", "")
    prop.setProperty("LOG_FORMAT_DELIM", ",")
    prop.setProperty("LOG_FORMAT_TYPE", "csv")
    prop.setProperty("LOG_MAX_LINES", "10000")
    prop.setProperty("LOG_SOCKET_APPENDER", "false")
    prop.setProperty("LOG_SOCKET_HOST", "localhost")
    prop.setProperty("LOG_SOCKET_PORT", "9899")
    prop.setProperty("LOG_SOCKET_RECONNECT_DELAY", "10000")
    prop.setProperty("LOG_THRESHOLD", "ALL")
//    prop.setProperty("KF_SERVER","CDTSSQL445P" )
//    prop.setProperty("KF_DATABASE","UK_EXP_MAIN_PROD_1.0.0.0" )
//    prop.setProperty("BT_SERVER","CDTSSQL445P" )
//    prop.setProperty("BT_DATABASE","UK_EXP_MAIN_PROD_1.0.0.0" )
//    prop.setProperty("KF_USER","BDF_UACC_USR")
//    prop.setProperty("SQL_PORT","1433")
//    prop.setProperty("KF_PWD","test")
//    prop.setProperty("PSCR_VIEW_NAME","dbo.vw_tbl_2_data")
//    prop.setProperty("RPT_IND_VALS","0,3")
    prop.setProperty("PRD_WK_CUTOFF_DAY","3")   //Values can be :  Mon-1, Tue-2, Wed-3, Thu-4, Fri-5, Sat-6, Sun-7
    prop.setProperty("PRD_WK_CUTOFF_HRS","5")   //24 hours format 0-23
    prop.setProperty("PRD_WK_CUTOFF_MIN","0")   //Minutes Ranges from 0-59
//    prop.setProperty("CEGIDIM_DEDUP_SUPP_PROC_ID", "CRXPMR")
//    prop.setProperty("CEGIDIM_DEDUP_PERIOD", "-60")
//    prop.setProperty("BDC_FCC_CORRECTION_TYPE", "PROD")
//    prop.setProperty("BDC_FCC_CARD_LEVEL_STATUS", "SUBMITTED")
//    prop.setProperty("RECOVERY_PERIOD", "-50000")
//    prop.setProperty("RECOVERY_OPRTNL_STAT_CODE","1")
//    prop.setProperty("DSPNSD_VIEW_NAME","dbo.vw_tbl_3_data")
    prop.setProperty("IMPALA_PWD","impala")
    prop.setProperty("ODS_ERROR_THRESHOLD_VALUE", "0")
    prop.setProperty("dk9_ndh_DATA_SUPPLIER","development/dk9/data/ndh/raw")
    prop.setProperty("dk9_ndh_DATA_SUPPLIER_REJ_SUBDIR", "rej")
    prop.setProperty("dk9_ndh_DATA_SUPPLIER_ARC_SUBDIR", "arc")
    prop.setProperty("dk9_ndh_DATA_SUPPLIER_ERR_SUBDIR", "err")
    prop.setProperty("dk9_ndh_DATA_SUPPLIER_ARC_ENABLED", "true")
    prop.setProperty("START_FROM_PERSISTED_OFFSET", "true")
    prop.setProperty("TRIGGER_EMAIL_ALERT","false")
    prop.setProperty("email.from", "noreply@iqvia.com")
    prop.setProperty("email.to", "Brahmdev.Kumar@imshealth.com")
    prop.setProperty("email.cc", "")
    prop.setProperty("email.bcc", "")
    prop.setProperty("email.smtpHost", "intemail.rxcorp.com")
    //Mock sftp
    prop.setProperty("SFTP_HOST","localhost")
    prop.setProperty("SFTP_PORT","22000")
    prop.setProperty("SFTP_USER", "dk9TestUser")
    prop.setProperty("SFTP_PASSWORD", "moveit.pwd")
    prop.setProperty("SFTP_TGTDIR","/development/dk9/data/ndh")
    prop.setProperty("SFTP_SRCDIR", "development/dk9/data/ndh/sftp")
//    prop.setProperty("SFTP_ARCDIR", "development/dk9/data/ndh/sftp/archive")
//    prop.setProperty("SFTP_REJECTDIR", "development/dk9/data/ndh/sftp/reject")

    //Mock sftp LMS
    prop.setProperty("LMS_SFTP_HOST","localhost")
    prop.setProperty("LMS_SFTP_PORT","22000")
    prop.setProperty("LMS_SFTP_USER", "dk9TestUser")
    prop.setProperty("LMS_SFTP_PASSWORD", "lmsftp.pwd")
    prop.setProperty("LMS_SFTP_SRCDIR", "development/dk9/data/ndh/sftp")
    prop.setProperty("LMS_PRD_FILE_PATTERN", "lms01.txt")
    prop.setProperty("LMS_PAC_FILE_PATTERN", "lms02.txt")
    prop.setProperty("LMS_COM_FILE_PATTERN", "lms09.txt")
    prop.setProperty("LMS_DRG_FILE_PATTERN", "lms21.txt")
//    prop.setProperty("BDC_APRV_TBL_RET_MONTH", "3")
//    prop.setProperty("BDC_CC_FILE_RETENTION_MTH", "3")
//    prop.setProperty("BDC_CC_PRODUCT_FILE_PREFIX", "dk9_bdc_ndh_product_")
//    prop.setProperty("BDC_CC_RI_FILE_PREFIX", "dk9_bdc_ndh_rptbl_rec_ind_")
//    prop.setProperty("BDC_CARD_STATUS", "SUBMITTED")
//    prop.setProperty("BDC_REPIND_CORRECTION_TYPE", "REPIND")
//    prop.setProperty("QC1_NO_OF_PERIODS","2")
//    prop.setProperty("QC1_REQ_NO_OF_PERIODS","2")
//    prop.setProperty("QC1_LOWER_WARNING_LIMIT","2")
//    prop.setProperty("QC1_UPPER_WARNING_LIMIT","2")
//    prop.setProperty("QC1_LOWER_ERROR_LIMIT","2.2")
//    prop.setProperty("QC1_UPPER_ERROR_LIMIT","3.8")
//    prop.setProperty("TOP_SUPPLIER_LST","123")
//    prop.setProperty("QC1_LATE_DATA_PERIODS","0")
//    prop.setProperty("TOP_SUPPLIER_CNT","1")
//    prop.setProperty("QC1_TST_OUTCM_OVRLL", "3,5")
//    prop.setProperty("QC1_TOP_SUPPLIER_CHKDAYS","7")
//    prop.setProperty("dk9_APPROVAL_UI_URL","https://ukwhsui-devl.rxcorp.com/")
//    prop.setProperty("SUMM_BUILD_PERIOD","24")
    prop.setProperty("BEM_BUSINESS_REST_URL", "https://event-api-devl.imshealthcloud.net/1.0/api/business-event")
    prop.setProperty("BEM_APPLICATION_REST_URL", "https://event-api-devl.imshealthcloud.net/1.0/api/application-event")
    prop.setProperty("BEM_LOGGER_TIMEOUT", "20000")
    prop.setProperty("PROD_SUPPORT_BLR_MAIL_ID", "EnterpriseProdOpssupport@in.imshealth.com")
    prop.setProperty("PROD_SUPPORT_CONTACT_MAIL_ID", "")
    //FSL
//    prop.setProperty("QC1_TEST_OUTCM_REC_CNT", "-2,-1,3,4")
//    prop.setProperty("Imputation_Allowed_Weeks", "6")
//    prop.setProperty("FSL_Good_Data_Weeks_check", "4")
//    prop.setProperty("FSL_Good_Data_Weeks_Max_Lookup", "12")
//    prop.setProperty("FSL_Good_Data_Txn_Count", "45")
//    prop.setProperty("FSL_Good_Data_Day_Count", "3")
//    prop.setProperty("PRD_WEEK_START_DAY", "Saturday")
//    prop.setProperty("PRD_WEEK_START_DAY_NBR", "6") //Values can be :  Mon-1, Tue-2, Wed-3, Thu-4, Fri-5, Sat-6, Sun-7
//    prop.setProperty("PRD_WEEK_NO_DEFAULT", "0")
    //SUMMARY BUILD
//    prop.setProperty("SUMMARY_BUILD_PERIOD", "12")`
//    prop.setProperty("SUMMARY_BUILD_ZZZ_FCC","95897,160399")
//    prop.setProperty("SUMMAY_BUILD_DFLT_REIMBMNT_TYP_CD","Z")
//    prop.setProperty("SUMMAY_BUILD_DFLT_REIMBMNT_TYP_DESC","Type to be determined")
//    prop.setProperty("SUMMARY_BUILD_CHEMIST_TYP","TET.WUK.RCH,TET.WUK.RCC")
    prop.setProperty("DWH_RECORDS_PER_FILE", "200000")
    prop.setProperty("SOURCE_RECORDS_PARTS", "20")
    //BDC REST API
    prop.setProperty("BDC_JWT_REST_URL", "http://localhost:9999/auth/ldap")
    prop.setProperty("BDC_DATA_REST_URL", "http://localhost:9999/cards")
    prop.setProperty("REST_CALL_TIMEOUT", "20000")
    prop.setProperty("BDC_WEB_UI_LINK", "https://ukndhui-devl.rxcorp.com/")
    prop.setProperty("REST_API_PASSWORD", "bdl.pwd")
    ConfigFactory.parseProperties(prop)
  }
}
