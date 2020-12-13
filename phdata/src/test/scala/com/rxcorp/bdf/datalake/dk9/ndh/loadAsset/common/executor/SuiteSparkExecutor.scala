//package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor
//
//import java.util.{ArrayList, List}
//
//import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.test.DddUnitTest
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import spray.json._
//
//import scala.collection.JavaConversions._
//import scala.concurrent.ExecutionContext.Implicits._
//import scala.concurrent.Future
//
///**
//  * Created by DJha on 18/04/2018.
//  */
//class SuiteSparkExecutor extends DddUnitTest {
//
//  // test starts
//  behavior of "SparkExecutor"
//  lazy val stage = "SparkExecutor test"
//  @transient private var sourceDf: DataFrame = _
//  @transient private var sparkExecutor: SparkExecutor = _
//  @transient private var spark: SparkSession = _
//  val topicName = "UK.WHS.SUPPLIER.LOADEVENT_TEST"
//
//  override def beforeAll() {
//    super.beforeAll()
//    sparkExecutor = appContext.getSparkExecutor(appContext.config, appContext.logger)
//    spark = sparkExecutor.asInstanceOf[TestSparkExecutor].getSparkSession()
//    kafka.createTopic(topicName)
//  }
//
//  it should "write and read data from kafka" in {
//
//    val newSupplierEvent1 = SupplierLoadedEvent(Math.random().toString, "Test", Option("Test2"), Option("Test3"))
//    sparkExecutor.kafka.sendEventToKafka(newSupplierEvent1.toJson.compactPrint, "1st test kafka message", topicName)
//    val newSupplierEvent2 = SupplierLoadedEvent(Math.random().toString, "Test", Option("Test2"), Option("Test3"))
//    sparkExecutor.kafka.sendEventToKafka(newSupplierEvent2.toJson.compactPrint, "2nd test kafka message", topicName)
//
//    val acc = sparkExecutor.kafka.getCollectionAccumulator[SupplierLoadedEvent]("events")
//    Future {
//      val rawDf = sparkExecutor.kafka.readFromKafka[SupplierLoadedEvent](topicName)
//      sparkExecutor.kafka.writeCollectionAccumulator(rawDf, "Messages read from Kafka", acc, 10000)
//    }
//
//    var processedEvents = 0
//    var maxLoops = 20
//    while(maxLoops > 0)
//    {
//      if (!acc.isZero) {
//        var events: List[SupplierLoadedEvent] = new ArrayList[SupplierLoadedEvent]
//        acc.synchronized({
//          events = acc.value
//          acc.reset()
//        })
//        events.distinct.foreach(event => {
//          println(s"The processed events value is ${processedEvents}, current event: ${event.supplierId}")
//          processedEvents += 1
//        })
//      }
//      maxLoops -= 1
//      Thread.sleep(2000)
//    }
//    assert(processedEvents == 2)
//  }
//}
