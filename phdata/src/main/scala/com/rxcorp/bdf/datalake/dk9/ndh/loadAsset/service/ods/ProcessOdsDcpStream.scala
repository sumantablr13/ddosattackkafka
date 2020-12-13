package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.ods

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical.NdhOds
object ProcessOdsDcpStream extends ProcessOdsDcpStreamTrait{
}

trait ProcessOdsDcpStreamTrait extends ProcessDcpStreamTrait  {
  override def getkafkaTopicName(): Seq[String] = {
    Seq(
      appContext.config.getString("KAFKA_TOPIC_DAQE_SST").trim,
      appContext.config.getString("KAFKA_TOPIC_DAQE_AMGROS").trim
    )
  }

}