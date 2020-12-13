package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common


import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.{SftpExecutor, SparkExecutor}

import com.typesafe.config.{Config, ConfigFactory}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException

import scala.collection.immutable

case class AppContext(
  config: Config,
  getSparkExecutor: (Config) => SparkExecutor,
  getSftpExecutor: (String,String,String,Int) =>SftpExecutor,
  database:String
)


/**
  * Companion object to assist construction of AppContext
  */
object AppContext extends AppContextTrait {
}

/**
 * Trait that helps us during testing
 */

trait AppContextTrait {
  @transient protected var _spark: SparkSession = _
  @transient protected var _sftpExecutor: SftpExecutor = _


  protected def getSparkSession(): SparkSession = SparkSession.builder()
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("hive.exec.max.dynamic.partitions", "10000")
    .enableHiveSupport().getOrCreate()
  protected def getConfig(): Config = ConfigFactory.load()

  protected def setupSparkSession(spark: SparkSession, assetDatabase: String): Boolean = {
    try {
      spark.sql("set hive.execution.engine=spark")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      spark.sql("set hive.exec.max.dynamic.partitions=10000")
      spark.sql("set hive.scratchdir.lock=false")
      spark.sql(s"use ${assetDatabase}")
      true
    } catch {
      /*Missing database*/
      case _: NoSuchDatabaseException => {
        //logger.warn(s"Database ${assetDatabase} does not exist!")
        true
      }
      case e: Exception => {
       // logger.logError(e)
        throw e
      }
    }
  }


  protected def getSparkExecutor(config: Config): SparkExecutor = {
    val dk9Database =  config.getString("dk9_DATABASE").trim
    val kafkaServer =  config.getString("KAFKA_SERVER").trim
    setupSparkSession( _spark, dk9Database)
    return new SparkExecutor( _spark,kafkaServer)
  }


  protected def getSftpExecutor(sftpHost:String,sftpUser:String,sftpPassword:String,sftpPortNo:Int): SftpExecutor = {
    if (_sftpExecutor != null) {
        _sftpExecutor = null
    }
    return new SftpExecutor( sftpHost, sftpUser, sftpPassword, sftpPortNo)
  }



  def apply(): AppContext = {
    _spark = getSparkSession()
    lazy val getDatabase =  config.getString("dk9_DATABASE").trim
    lazy val config = getConfig()

    new AppContext(
      config
      , getSparkExecutor
      , getSftpExecutor
      , getDatabase
    )
  }
}
