package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.dwh

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, AppLogger, KafkaOffsetPersistance, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical.OdsLoadedEvent
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.dwh.DwhProcessor
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.EmailAlerts
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh.{DwhEmailStats, DwhEmailStatsInitialization, ProcessDwhStreamConstants}
import org.joda.time.DateTime

case class DwhLoadProcess(
                           appContext: AppContext,
                           odsProcessors: List[DwhProcessor],
                           kafkaOffsetPersistance: KafkaOffsetPersistance,
                           emailAlerts: EmailAlerts
                         ) {

  def execute(event: OdsLoadedEvent, topic: String, partition: Int, offset: Long): Unit = {
    // 1. list processors
    DwhEmailStats.dwhStartTime = new DateTime()

    // 2. process incoming data
    // TODO: verify event message structure - especially data_typ_shrt_nm
    odsProcessors.filter(_.isProcessing(event.src_impl_pit_id, event.src_proc_pit_id, event.data_typ_shrt_nm))
      .foreach(dwhProcessor => {
        val processedRecordsCount = dwhProcessor.process(event)
        emailAlerts.sendDwhSuccessStatsEmail(
          event.impl_btch_pit_id,
          event.src_proc_pit_id,
          event.dervd_dsupp_file_tm_pd_nbr,
          Option(event.supld_dsupp_proc_id).getOrElse("--"),
          processedRecordsCount.recordCount.toString,
          topic,
          offset.toString
        )
      })

    //3. update kafka offset
    kafkaOffsetPersistance.saveOffsetData(topic, partition, ProcessDwhStreamConstants.PROCESS_NAME, offset)
  }
}
