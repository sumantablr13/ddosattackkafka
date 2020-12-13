package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.rds.physical.shared

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.model.Table
import org.apache.spark.sql.types._

abstract class AbstractRdsOacPharmcyUnvrs(appContext: AppContext) extends Table("") {
  override lazy val logicalName: String = "RdsPharmacyUniverse"
  override lazy val physicalName: String = "v_oac_pharmy_unvrs"

  override lazy val tableColumns: Array[(String, String)] = Array(
      ("oac_id", "STRING")
    , ("loc_id", "BIGINT")
    , ("org_id", "BIGINT")
    , ("org_opunit_nm", "STRING")
    , ("ddms_id", "STRING")
    , ("bus_eff_ts", "STRING")
    , ("bus_expry_ts", "STRING")
    , ("publ_eff_ts", "STRING")
    , ("publ_expry_ts","STRING")
  )


  override lazy val schema: StructType = (new StructType)

    .add("oac_id", StringType)
    .add("loc_id", LongType)
    .add("org_id", LongType)
    .add("org_opunit_nm", StringType)
    .add("ddms_id", StringType)
    .add("bus_eff_ts" ,StringType)
    .add("bus_expry_ts" ,StringType)
    .add("publ_eff_ts" ,StringType)
    .add("publ_expry_ts" ,StringType)


  override lazy val createTableOpts: String =
    """
      STORED AS PARQUET
      TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """.stripMargin
}
