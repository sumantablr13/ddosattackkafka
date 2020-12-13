package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.PartitionTable
import org.apache.spark.sql.types._

abstract class AbstractPbsSupplierFileLoadValidation(appContext: AppContext) extends PartitionTable(""){

  override lazy val logicalName: String = "Drug Distribution Data Duplicate File Validation"
  override lazy val physicalName: String = "dsupp_file_load_vld_log"

  override lazy val tableColumns: Array[(String, String)] = Array(
    ("ctry_iso_cd", "STRING")
  , ("data_asset_nm", "STRING")
  , ("data_cntnt_catg_cd", "STRING")
  , ("src_sys_impl_id", "STRING")
  , ("src_sys_impl_btch_id", "STRING")
  , ("dsupp_file_nm", "STRING")
  , ("dsupp_file_tm_pd_nbr", "INT")
  , ("dsupp_file_tm_pd_typ_nm", "STRING")
  , ("dsupp_file_load_stat_cd", "STRING")
  , ("dsupp_file_dupe_ind", "STRING")
  , ("dsupp_file_md5sum_txt", "STRING")
  , ("dsupp_file_rec_cnt", "INT")
  , ("meas1_attr_nm", "STRING")
  , ("meas1_attr_val", "DECIMAL(17 3)")
  , ("meas2_attr_nm", "STRING")
  , ("meas2_attr_val", "DECIMAL(17 3)")
  , ("appl_id", "STRING")
  , ("proc_eff_ts", "TIMESTAMP")
  , ("proc_expry_ts", "TIMESTAMP")
  , ("proc_by_nm", "STRING")
  )

  override lazy val partitionColumns: Array[(String, String)] = Array(("dervd_dsupp_proc_id", "STRING"))


  override lazy val schema: StructType =
    (new StructType)
      .add("ctry_iso_cd",StringType)
      .add("data_asset_nm",StringType)
      .add("data_cntnt_catg_cd",StringType)
      .add("src_sys_impl_id",StringType)
      .add("src_sys_impl_btch_id",StringType)
      .add("dsupp_file_nm",StringType)
      .add("dsupp_file_tm_pd_nbr",IntegerType)
      .add("dsupp_file_tm_pd_typ_nm",StringType)
      .add("dsupp_file_load_stat_cd",StringType)
      .add("dsupp_file_dupe_ind",StringType)
      .add("dsupp_file_md5sum_txt",StringType)
      .add("dsupp_file_rec_cnt",IntegerType)
      .add("meas1_attr_nm",StringType)
      .add("meas1_attr_val",DecimalType(17,3))
      .add("meas2_attr_nm",StringType)
      .add("meas2_attr_val",DecimalType(17,3))
      .add("appl_id",StringType)
      .add("proc_eff_ts",TimestampType)
      .add("proc_expry_ts",TimestampType)
      .add("proc_by_nm",StringType)
      .add("dervd_dsupp_proc_id",StringType)

  override lazy val createTableOpts: String =
    """
       |ROW FORMAT DELIMITED
       |FIELDS TERMINATED BY '\001'
       |LINES TERMINATED BY '\n'
       |STORED AS TEXTFILE
  """.stripMargin

  def helperCreatePartitionSpec(dervdDsuppProcId: String): String = s"dervd_dsupp_proc_id='${dervdDsuppProcId}'"
}
