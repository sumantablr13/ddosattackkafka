package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.{DdosAttackHys, MDataKafkaOffset}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SparkSqlQuery
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service.SparkServiceTrait
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{KafkaOffsetPersistance, TimestampConverter}
import org.apache.spark.sql.functions._




trait ServiceAtackSys  extends SparkServiceTrait {
  val kafkaTable = new MDataKafkaOffset(appContext)
  @transient protected var _spark: SparkSession = _


  val kafkaOffsetPersistance = new KafkaOffsetPersistance(appContext, sparkExecutor.sql, kafkaTable)
  var event: String = ""
  var topic: String = ""
  def main(args: Array[String]): Unit = {
    println("I am in")
    try {
      // log file folder location
      val fileLogLoc = appContext.config.getString("APPACHE_LOG_LOC")

      // file name lost
      val file = getListOfFiles(fileLogLoc)

      val fileName = if (file.size > 0) {
        file(0).toString
      }
      else {
        throw new Exception("Log file not available!!!!")
      }

      //get the log file and convert to DF


      val inputFileLogData = getFileData(fileLogLoc + "" + fileName, "-")
      val dfFileLogDataSchema = inputFileLogData.toDF("ipAddress", "noVal", "dateDs", "description")
      //val dfFileLogDataSchema =
      //dfSchema.select(substring(col("http_add"),1,instr(col("http_add"),"+").toString().toInt-1))
      dfFileLogDataSchema.createOrReplaceTempView("tmp")

      // select ip address,time stamp and file name
      val dfFileLogIpAndTime = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
        override val logMessage: String = s""
        override val sqlStatement: String =
          s"""SELECT ipAddress,substr(trim(dateDs),2,instr(trim(dateDs),'+')-2) as dateDs,'${fileName}' as fileName  from  tmp limit 70
        """.stripMargin
      })

      //Send the kafka event based on the log DataFrame
      sendKafkaEvent(dfFileLogIpAndTime)

    } catch {
      case e: Exception => {
        println("Exception:" + e.getMessage)
      }

    }
  }

  final protected def getFileData(filePath: String, delimiter: String) = {
    val accessLogRdd = sparkExecutor.file.getDataFromFille(filePath, delimiter)
    accessLogRdd
  }

  final protected def sendKafkaEvent(dfFileLogIpAndTime: DataFrame): Boolean = {

    // Get the topic name
    val TopicName = appContext.config.getString("KAFKA_TOPIC_PH_DATA")
     //Per topic how many records we have to convert Json string
    val slicePerTopic = appContext.config.getString("SLICE_PER_TOPIC")

    //Convert to json string
    val lsFileLogIpAndTime = dfFileLogIpAndTime.select("ipAddress", "dateDs", "fileName").toJSON.collect().toList


    val parSlice = slicePerTopic.toInt
    var strtIndex = 0
    var endIndex = parSlice
    // Calculate the total topic we have to send
    var totalSlice = sliceCount(slicePerTopic.toInt, lsFileLogIpAndTime.size)

    //Iterate the total slice have to process
    for (i <- 1 to totalSlice) {
      println("Slice Index::" + i)
      endIndex = strtIndex + parSlice
      println("strtIndex::" + strtIndex)
      println("endIndex::" + endIndex)

      //  Event creation from List slice
      val strEvent = s"""${lsFileLogIpAndTime.slice(strtIndex, endIndex).mkString(",")} """.stripMargin
      strtIndex = endIndex
      println(strEvent)
      // Send the kafka event
      sparkExecutor.kafka.sendEventToKafka(strEvent, "Kafka message send", TopicName)

      //Read the event
      readEvent()

    }

    true
  }

  protected final def sliceCount(slicePerTopic: Int, totalRecodCnt: Int): Int = {
    var totalSlice = totalRecodCnt / slicePerTopic
    println(totalSlice)

    if ((totalRecodCnt - totalSlice) > 0) {
      totalSlice = totalSlice + 1
    }
    totalSlice
  }

  final protected def readEvent() = {

    var kafkaTopicName = appContext.config.getString("KAFKA_TOPIC_PH_DATA").trim
    val DdosFileLoc = appContext.config.getString("DDOS_FILE_LOC").trim
    val ddosProcessName = appContext.config.getString("KAFKA_DDOS_PROCESS_NAME").trim



    // Take the intervel time for DDOS attack
    val intervelTime = appContext.config.getString("INERVEL_TIME").trim
    var offset = 0
    var partition = 0
    val ddottackHys = new DdosAttackHys(appContext)

    // Already process Kafka topic
    var availableOffset = kafkaOffsetPersistance.readOffsetData(kafkaTopicName, ddosProcessName)


    // Read the kfaka event
    val kafkaMessage = sparkExecutor.kafka.readSingleKafkaEvent(kafkaTopicName, availableOffset)

    val eventCount = kafkaMessage.count()
    println(s"Received ${eventCount} events")
    if (eventCount > 0) {
      try {

        val kafkaMessage1 = kafkaMessage.collect().map(events => (events._1, events._2, events._3, events._4))
        println(":Json::" + kafkaMessage1(0)._1)
        val getJsonMsgString = kafkaMessage1(0)._1.split("\\}\\,").toSeq.map(x => x.concat("}"))
        kafkaTopicName = kafkaMessage1(0)._2
        partition = kafkaMessage1(0)._3.toInt
        //Get the current offset number
        offset = kafkaMessage1(0)._4.toInt

        //Convert json string to DataFrame
        val getMsgDf = sparkExecutor.file.convertJsonToDF(getJsonMsgString)

        getMsgDf.show(false)

        //Convert dataFrame date string to unix timestamp
        val dsApplyDateFormat = getMsgDf.select(col("ipAddress"),
          col("fileName"),
          from_unixtime(unix_timestamp(col("dateDs"), "dd/MMM/yyyy:HH:mm:ss")).as("dateDs"))
          .select(ddottackHys.schema.map(field => col(field.name)).toArray: _*)

        //Get current file name
        val getFileName = dsApplyDateFormat.select("fileName").head().get(0).toString()

        //Get History log file
        val getHysData = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
          override val logMessage: String = s"Get log history transaction"
          override val sqlStatement: String =
            s"""SELECT * from ph_data.${ddottackHys.physicalName} where fileName = '${getFileName}'
        """.stripMargin
        }).select(ddottackHys.schema.map(field => col(field.name)).toArray: _*)



        //Current offset message dataframe and History log dataFrame union
        val hysAndNewRecords = dsApplyDateFormat.union(getHysData)

        hysAndNewRecords.createOrReplaceTempView("tmp1")

        // Lag calculation between same address timestamp
        val getDdosAttckStats = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
          override val logMessage: String = s""
          override val sqlStatement: String =
            s"""select ipAddress,unix_timestamp(dateDs) - unix_timestamp(new_dt) as diff from (SELECT ipAddress,dateDs, lag(dateDs, 1) over(partition by ipAddress order by dateDs) as new_dt,fileName  from  tmp1) as tblo
        """.stripMargin
        })
        println("Total Records::" + hysAndNewRecords.count())
        println("DDos Attck records::" + getDdosAttckStats.filter("diff > "+intervelTime+"").count())

        // Current kafka offset logs insert to history log table
        sparkExecutor.sql.insertTable(ddottackHys, dsApplyDateFormat, s"Mocked existing source data for", "PH", "", new TimestampConverter())


        //Old and current ddos ipaddress get

        val ddosAttacjIpAddress = if(getFileData(DdosFileLoc, ",").count()>0)getFileData(DdosFileLoc, ",").toDF("ipAddress")
          .union(getDdosAttckStats.filter("diff > " + intervelTime + "").select("ipAddress")).distinct()
        else
          getDdosAttckStats.filter("diff > " + intervelTime + "").select("ipAddress").distinct()




        //Write the DDOS attack ipaddress to file
        val file = new File(DdosFileLoc)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(ddosAttacjIpAddress.collect().mkString("\n").replace("[", "").replace("]", ""))
        bw.close()

        // Save the last offset to kafka transaction table
        kafkaOffsetPersistance.saveOffsetData(kafkaTopicName, partition, ddosProcessName, availableOffset)

      }
      catch {
        case e: Exception => {
          println("Incorrect message format " + event)
          println("Exp::" + e.getMessage)
        }
          //appContext.logger.logError(e)
          throw e
      }
    }
    else
      throw new Exception("Kafka msg not consume by process!!!!")
  }


  final protected def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".txt"))
      .map(_.getName).toList

  }

  final protected def getListOfFiles1(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


}
