package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.ods.logical

trait StandardSupplierToString { this:StandardSupplierFormat =>

  override def toString: String = 
    dervd_dsupp_trans_id
}
