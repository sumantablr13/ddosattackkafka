package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.Timestamp

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.KafkaOffsetPersistance
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service.SparkServiceTrait
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.{ComputerAttack, DAQELoadedEvent}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh.{DwhEmailStats, ProcessDwhStreamConstants}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import spray.json._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.metadata.spark.MDataKafkaOffset
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.query.SparkSqlQuery
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service.SparkServiceTrait
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{BemConf, KafkaOffsetPersistance}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.exception.{FileSizeCountException, ZeroDRecordsFoundException}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.DAQELoadedEvent
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.SrcKafkaEventProtocol._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.ods.MappingProcess
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.supplierPreProcessor.{FileFormatSSTTbl1PreProcessor, FileFormatSSTTbl2PreProcessor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.{EmailAlerts, FileContentValidation, ValidationRunner}
import kafka.message.InvalidMessageException
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType, StructType}
import org.joda.time.DateTime
import spray.json._

import scala.collection.mutable
import scala.util.Try
trait ServiceAtackSys  extends SparkServiceTrait{
  val kafkaTable = new MDataKafkaOffset(appContext)
  @transient protected var _spark: SparkSession = _
  protected val emptyDS: Dataset[String] = sparkExecutor.sql.emptyDatasetString
  import emptyDS.sqlContext.sparkSession.implicits._

  val kafkaOffsetPersistance = new KafkaOffsetPersistance(appContext, sparkExecutor.sql, kafkaTable)
  var event: String = ""
  var topic: String = ""
  var partition: Int = 0
  var offset: Long = 0
  def main(args: Array[String]): Unit = {
    println("I am in")
    val inputData=getFileData()

    final private val normToFileCols = Array[String](
      "correct_card_assct_tkt_nbr",
      "dsupp_proc_id",
      "dspnsing_oac_lgcy_id",
      "dspnsing_oac_id",
    ).map(colName => col(colName))
    val dfSchema=inputData.toDF("","noVal","http_add","description")
    //dfSchema.select(substring(col("http_add"),1,instr(col("http_add"),"+").toString().toInt-1))
    dfSchema.createTempView("tmp")
    val sstTbl1DataDF = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
      override val logMessage: String = s""
      override val sqlStatement: String =
        s"""SELECT ipAddress,substr(trim(http_add),2,instr(trim(http_add),'+')-2) as http_add from  tmp limit 5000
        """.stripMargin
    })

    sstTbl1DataDF.toJSON
   // dfSchema.where("description like '%Service Pack%'").show(5,false)

    //substr(http_add,1,instr(http_add,'+')-1)

    val lst=List(1,2,3)
    println(lst.size)
   for(ls<-lst){
    sendKafkaEvent(ls.toString,sstTbl1DataDF)
    }


    println("Send kafka event")
    Thread.sleep(60)
    println("Send kafka event wait")
   val kafkaMessage= readEvent1(0,lst.size-1)
    processMultiEvents(kafkaMessage)
    println("Read kafka event")
  }

  case class computerAttacField(ipAdress:String,des:String)
  final protected def getFileData()=
  {
    val accessLogRdd= sparkExecutor.file.getDataFromFille("C:/Users/Sumanta.Banik/Desktop/New folder (2)/apache-access-log-Copy.txt")
    //val rds=accessLogRdd.flatMap(x=>x.split(" "))
    accessLogRdd
   // println("Rdd")
  }
  final protected def sendKafkaEvent(data:String,sstTbl1DataDF:DataFrame): Boolean = {

    val TopicName = appContext.config.getString("KAFKA_TOPIC_DWH_INPUT")
    val ts=sstTbl1DataDF.select("ipAddress","http_add").toJSON.collect().toList
    println(ts(0).toString)
    val odsEvent = s"""${ts.mkString(",")} """.stripMargin
/*
    val odsEvent = s"""{
                            "src_impl_pit_id":"$data",
                            "src_proc_pit_id":"SOURCE",
                            "data_typ_shrt_nm":"pbs_si_trans_acq_ods.csv",
                            "impl_btch_pit_id":"sb"
                   }""".stripMargin
*/

   /* val odsEvent =
      s"""{
       "src_impl_pit_id":6834561356571316030,
       "src_proc_pit_id":"",
       "data_typ_shrt_nm":803,
       "impl_btch_pit_id":"multipleColumns"
       }
""".stripMargin*/

    println("Event creation")
    println(odsEvent)

    sparkExecutor.kafka.sendEventToKafka( odsEvent, "ODS test kafka message no 1", TopicName)



    true
  }

  final protected def readEvent1(i: Long,totalOffset:Long):Dataset[(String, String, Int, Long)] = {
    println(s"Checking for events (DWH)")

    val kafkaTopicName = appContext.config.getString("KAFKA_TOPIC_DWH_INPUT").trim
    val kafkaMessage = sparkExecutor.kafka.readMultiKafkaEvents(kafkaTopicName, "", i, totalOffset)
    //streamTopic: String, customKafkaServer: String, offset: Long, mbtMultiKafkaEventsToRead: Long = 0


    val eventCount = kafkaMessage.count()
    println(s"Received ${eventCount} events")
    if (eventCount > 0) {
      try {


        //(event.parseJson.convertTo[ComputerAttack](jsonReader), topic, partition, offset)
        val dd1= kafkaMessage.collect().map(ev=>ev._1).foreach{

          x=>{

            // x.parseJson.convertTo[ComputerAttack](jsonReader)
            println(x)
            /* val idSchema = new StructType().add("id", MapType(StringType, StringType))
             val idFieldsSchemaParser = sparkExecutor.file.readJsonAsDataFrame(x, idSchema)
             idFieldsSchemaParser.show(false)
             idFieldsSchemaParser.printSchema()*/
          }
        }
        val kafkaMessage1 = kafkaMessage.collect().map(events => (events._1, events._2, events._3, events._4))
        val mySeq = Seq("a","b")
        val mySeq1 = Seq("a","b")
        val ww= mySeq1 :+ mySeq
        val ls=List(0)

        val ds=for (i <- ls) {
          println(":Json::"+kafkaMessage1(i)._1)
          val aa=kafkaMessage1(i)._1.split("\\}\\,").toSeq.map(x=>x.concat("}"))
          val p=sparkExecutor.file.convertJsonToDF(aa)

          p.show(false)

         val ds= p.select(col("ipAddress"),from_unixtime(unix_timestamp(col("http_add"), "dd/MMM/yyyy:HH:mm:ss")).as("dateDs"))
          ds.show(false)

          ds.createTempView("tmp1")

          val sstTbl1DataDF = sparkExecutor.sql.getDataFrame(new SparkSqlQuery {
            override val logMessage: String = s""
            override val sqlStatement: String =
              s"""select ipAddress,unix_timestamp(dateDs) - unix_timestamp(new_dt) as diff from (SELECT ipAddress,dateDs, lag(dateDs, 1) over(partition by ipAddress order by dateDs) as new_dt  from  tmp1) as tblo
        """.stripMargin
          })
          sstTbl1DataDF.orderBy(col("ipAddress").asc).show(50,false)
          println("")

         // kafkaMessage1(i)._1.parseJson.convertTo[ComputerAttack](jsonReader))
        }




        return kafkaMessage
      }
      catch {
        case e: Exception => {
          println("Incorrect message format " + event)
          println("Exp::"+e.getMessage)
        }
          //appContext.logger.logError(e)
          throw e
      }
    }
    else
      return (null)
  }

  final protected def readEvent(i: Long,totalOffset:Long):Dataset[(String, String, Int, Long)] = {
    println(s"Checking for events (DWH)")

    val kafkaTopicName = appContext.config.getString("KAFKA_TOPIC_DWH_INPUT").trim
    val kafkaMessage = sparkExecutor.kafka.readMultiKafkaEvents(kafkaTopicName, "", i, totalOffset)
    //streamTopic: String, customKafkaServer: String, offset: Long, mbtMultiKafkaEventsToRead: Long = 0


    val eventCount = kafkaMessage.count()
    println(s"Received ${eventCount} events")
    if (eventCount > 0) {
      try {


        //(event.parseJson.convertTo[ComputerAttack](jsonReader), topic, partition, offset)
     val dd1= kafkaMessage.collect().map(ev=>ev._1).foreach{

         x=>{

          // x.parseJson.convertTo[ComputerAttack](jsonReader)
           println(x)
          /* val idSchema = new StructType().add("id", MapType(StringType, StringType))
           val idFieldsSchemaParser = sparkExecutor.file.readJsonAsDataFrame(x, idSchema)
           idFieldsSchemaParser.show(false)
           idFieldsSchemaParser.printSchema()*/
         }
      }
        val kafkaMessage1 = kafkaMessage.collect().map(events => (events._1, events._2, events._3, events._4))
     val mySeq = Seq("a","b")
     val mySeq1 = Seq("a","b")
    val ww= mySeq1 :+ mySeq
      val ls=List(0,1)
     val lsp:mutable.HashSet[ComputerAttack]=new mutable.HashSet[ComputerAttack]()

     val ds=for (i <- ls) {
       println(":::>"+kafkaMessage1(i)._1.parseJson.convertTo[ComputerAttack](jsonReader).toString)
       lsp.add(kafkaMessage1(i)._1.parseJson.convertTo[ComputerAttack](jsonReader))
        }
     println(">>>>>>>>"+ds)
     println(">>>lsp>>>>>"+lsp.toSeq)
    //_spark.sparkContext.parallelize(Array(1,2,3)).toDF().show(false)
    val dd= sparkExecutor.file.getRdd(lsp.toSeq)
    /*  _spark.sparkContext.parallelize(lsp.toSeq)
     _spark.createDataFrame( _spark.sparkContext.parallelize(lsp.toSeq))
*/
    val file = new File("C:\\Users\\Sumanta.Banik\\Desktop\\New folder (2)\\c.txt")
     val bw = new BufferedWriter(new FileWriter(file))
     bw.write(lsp.mkString("\n"))
     bw.close()
     dd.toDF()

      //df.write.mode('overwrite').parquet("/output/folder/path")


        return kafkaMessage
      }
      catch {
        case e: Exception => {
          println("Incorrect message format " + event)
          println("Exp::"+e.getMessage)
        }
          //appContext.logger.logError(e)
          throw e
      }
    }
    else
      return (null)
  }


  final protected def processMultiEvents(kafkaMessageData: Dataset[(String, String, Int, Long)]) = {

    println(kafkaMessageData)
    try{
      val kafkaMessage = kafkaMessageData.collect().map(events => (events._1, events._2, events._3, events._4))
     val sizeMsg= kafkaMessage.size
      println(s"Size is ${kafkaMessage.size}")
      println("Mlbt New Changes1")
      println(s"Kafka messages are ${kafkaMessageData.collect}")
      val i:Int=0;
      /*for (i=0;  i<sizeMsg;i++) {*/
        event = kafkaMessage(i)._1
        topic = kafkaMessage(i)._2
        partition = kafkaMessage(i)._3
        offset = kafkaMessage(i)._4
        println(
          s"""
              Received:
                event     : $event
                topic     : $topic
                partition : $partition
               offset     : $offset
          """.stripMargin
       )

        /*val businessKeysHashSchemaParser = sparkExecutor.file.readJsonAsDataFrame(event)
        ccAssctTktNbr = Try(businessKeysHashSchemaParser.select("businessKeysHash").head().getLong(0)).getOrElse(ccAssctTktNbr)*/


        val idFieldsSchemaParser = event.parseJson.convertTo[ComputerAttack](jsonReader)
        println(idFieldsSchemaParser.data_typ_shrt_nm)


      //}
    }catch {
      case e: Exception =>
        println("Incorrect message format "+e.getMessage)
        throw e
    }




}


}
