package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

object BemConf {

  val UNDEFINED=""

  case object EventMode {
    val APPLICATION_EVENT = "app"
    val BUSINESS_EVENT = "bus"
  }

  case object EventOutcome{
    val SUCCESS="success"
    val WARNING="warning"
    val FAILURE="failure"
  }

  case object EventType {
    val STARTED_EVENT = "started event"
    val FINISHED_EVENT = "finished event"
  }

  case object EventDomain {
    val DATA_EXTRACTION_COLLECTION = "Data Extraction and Collection"
    val DATA_AQUISITION = "Data Acquisition"
    val BRIDGING = "Bridging"
    val DATAWAREHOUSE = "Data Warehouse"
    val REPORT_DEFINITION_EXTRACTION = "Report Definition and Extraction"
    val REFERENCE_DATA = "Reference data"
    val DATA_CHANGE_CAPTURE = "Data change capture"
    val DATA_QUALITY = "Data Quality"
    val BUSINESS_EVENT_BUS = "Business event bus"
  }

  case object EventOperation {

    // "Data Extraction and Collection"
    val DCP_DATA_LOAD = "data load"
    val DCP_DATA_TRANSFER = "data transfer"
    //val DCP_FILE_DECRYPTION = "file decryption"

    // Data Acquisition domain
    val ODS_HEADER_TRAILER_VALIDATION = "header trailer validation"
    val ODS_BAD_RECORD_VALIDATION = "bad record validation"
    val ODS_DDUP_VALIDATION = "Duplicate record validation"
    val ODS_SRC_FILE_EXISTENCE_VALIDATION = "file existence validation"
    val ODS_NORMALIZATION = "data normalization"
    val ODS_BUSINESS_RULE_VALIDATION = "business rule validation"
    val ODS_SCHEMA_FORMATTING = "schema formatting"
    val ODS_FILE_READ = "source Data read"
    // for future use:
    //val ODS_DATA_TYPE_FORMATTING = "data type formatting"
    //val ODS_DEDUPLICATION_OF_RECORD = "deduplication of record"

    // Bridging Domain
    val ODS_PRODUCT_BRIDGING = "product bridging"
    val ODS_LOCATION_BRIDGING = "location bridging"
    val ODS_WRITE = "write into ods table"
    val ODS_ARCH_FILE = "archive input file"

    // Data Warehouse domain
    // A.Regular DWH process
    val DWH_ODS_DELTA = "delta from ods"
    val DWH_ODS_NORMALIZATION = "ods data normalization"
    val DWH_BI_TEMPORAL = "bi-temporalization"
    val DWH_WRITE = "write into dwh table"
    // Recovery
    val DWH_RCV_DELTA = "delta for recovery"
    val DWH_RCV_NORMALIZATION = "dwh data recovery"

    // Rebridging
    val DWH_REBRDG_DELTA = "delta for rebridging"
    val DWH_REBRDG_NORMALIZATION = "dwh data rebridged"

    // B. QC1
    val DWH_QC1_FIND_IN_PERIODS = "find time periods"
    val DWH_QC1_METRICS_TRANS = "metrics for transactions"
    val DWH_QC1_METRICS_PROD = "metrics for products"
    val DWH_QC1_METRICS_LOC = "metrics for locations"
    val DWH_QC1_METRICS_AGG = "metrics pre aggregation"
    val DWH_QC1_METRICS_REGION = "metrics for regions"
    val DWH_QC1_METRICS_COUNTRY = "metrics for country"
    val DWH_QC1_METRICS_OVERALL = "final metrics"
    val DWH_QC1_METADATA_SIGNOFF = "metadata sign offs"
    // for future use:
    val DWH_IMPUTATION = "imputation"
    val DWH_PROJECTION = "projection"
    val DWH_BDC_PRODUCT = "bdc for product"
    //Pharmacy Relocation
    val OAC_RLC_PROC_START = "processor identification"
    val OAC_RLC_SOURCE = "oac association source list"
    val OAC_RLC_MAP = "oac mapping process"
    val OAC_RLC_NORM = "oac map normalization"
    val OAC_RLC_WRITE = "write to table"
    val OAC_RLC_SEND_MAIL = "oac mapping status email"
    //fsl
    val DWH_FSL_PROC_START = "processor identification"
    val DWH_FSL_WEEKS = "weeks identification"
    val DWH_FSL_PANEL = "rx pharmacy universe list"
    val DWH_FSL_QC1_STAT = "qc1 pharmacy status"
    val DWH_FSL_IMP_STAT = "pharmacy six week imputation status check"
    val DWH_FSL_GD_DATA = "good data check"
    val DWH_FSL_NORM = "fsl data normalization"
    val DWH_FSL_WRITE = "write to table"
    val DWH_SEND_MAIL = "fsl status email"
    val DWH_FSL_OAC_RELOC = "pharmacy relocation mapping retrival"
    // DWH-Imputation
    val DWH_IMPTN_DELTA = "delta from imputation"
    val DWH_IMPTN_NORMALIZATION = "imputation data normalization"
    // DWH-BDC-FCC
    val DWH_BDC_FCC_DELTA = "delta from bdc fcc Correction card Table"
    val DWH_BDC_FCC_NORMALIZATION = "bdc fcc data normalization"
    // DWH-BDC-RI
    val DWH_BDC_RI_DELTA = "delta frm bdc ri Correction card Table"
    val DWH_BDC_RI_NORMALIZATION = "bdc ri data normalization"
    //summarybuild
    val DWH_SUMM_BLD_GET_PRD_CYLE = "Get Prod Cycle PrdNbr"
    val DWH_SUMM_BLD_GET_TMPD_DATA = "Get TimePeriod Data"
    val DWH_SUMM_BLD_GET_PACK_DATA = "Get Pack Data"
    val DWH_SUMM_BLD_GET_LOC_DATA = "Get Location Data"
    val DWH_SUMM_BLD_GET_REIMBMNT_DATA = "Get Reimbursement Data"
    val DWH_SUMM_BLD_GET_BNDRY_HEIR_DATA = "Get Bndry Heir Data"
    val DWH_SUMM_BLD_GET_PRJTN_OUT_DATA = "Get Projection Data"
    val DWH_SUMM_BLD_GET_FSL_OUT_DATA = "Get FSL Data"
    val DWH_SUMM_BLD_GET_TRNS_DATA = "Get Transaction Data"
    val DWH_SUMM_BLD_PARTN_SWTCH_DATA = "Partition Switch"
    val DWH_SUMM_BLD_INSRT_DATA = "Load data"
    //production_cycle
    val DWH_PROD_CYLE_START = "Production Cycle Start"
    val DWH_PROD_CYLE_END = "Production Cycle End"
    val DWH_PROD_WEEKLY_CYLE_START = "WEEKLY Production Cycle Start"
    val DWH_PROD_WEEKLY_CYLE_END = "WEEKLY Production Cycle End"
    val DWH_MONTHLY_PROD_CYLE_START = "MONTHLY Production Cycle Start"
    val DWH_MONTHLY_PROD_CYLE_END = "MONTHLY Production Cycle End"
    val DWH_SIGN_OFF_START = "Sign Off Process"
    val DWH_WEEKLY_SIGN_OFF_START = "WEEKLY Sign Off Process"
    val DWH_MONTHLY_SIGN_OFF_START = "MONTHLY Sign Off Process"
    val DWH_PROD_CYLE_END_LOAD = "Loading Data"
    //BDC
    val DWH_BDC_FILE_READ = "Source File Read"
    val DWH_BDC_NORMALIZATION = "Data Normalization"
    val DWH_BDC_VALIDATION = "BDC record validation"
    val DWH_BDC_CC_INSRT_DATA = "Load data into CC tbl"
    val DWH_BDC_CC_ARCHIVE_FILE = "Archive file"
    val DWH_BDC_ARCHIVE_DATA = "Load data into Archive tbl"
    val DWH_BDC_APPR_TBL_LOAD = "Load data into approval tbl"

    //Product Reference load
    val REF_PRD_PROC_START = "processor identification"
    val REF_PRD_READ_FTP= "source data normalization"
    val REF_PRD_NORM = "fsl data normalization"
    val REF_PRD_WRITE = "write to table"
    val REF_PRD_SEND_MAIL = "load status email"

  }

  case object EventLevel {
    val ERROR = "ERROR"
    val WARN = "WARN"
    val INFO = "INFO"
    val DEBUG = "DEBUG"
  }

  def getEventScopeAttrName(eventDomain: String): String = {
    eventDomain match {
      case EventDomain.DATA_EXTRACTION_COLLECTION => "supplierScope"
      case EventDomain.DATA_AQUISITION => "supplierScope"
      case EventDomain.BRIDGING => "supplierScope"
      case EventDomain.DATAWAREHOUSE => "offeringType"
      case EventDomain.REFERENCE_DATA => "offeringType"
      case EventDomain.DATA_CHANGE_CAPTURE => "offeringType"
      case EventDomain.DATA_QUALITY => "offeringType"
      case EventDomain.REPORT_DEFINITION_EXTRACTION => "clientScope"
      case EventDomain.BUSINESS_EVENT_BUS => "clientScope"
    }

  }

  def getEventDomain(eventOperation: String): String = {
    eventOperation match {
      case EventOperation.DCP_DATA_TRANSFER => EventDomain.DATA_EXTRACTION_COLLECTION
      case EventOperation.DCP_DATA_LOAD => EventDomain.DATA_EXTRACTION_COLLECTION
      case EventOperation.ODS_WRITE => EventDomain.BRIDGING
      case EventOperation.ODS_ARCH_FILE => EventDomain.BRIDGING
      case EventOperation.ODS_HEADER_TRAILER_VALIDATION => EventDomain.DATA_AQUISITION
      case EventOperation.ODS_BAD_RECORD_VALIDATION => EventDomain.DATA_AQUISITION
      case EventOperation.ODS_DDUP_VALIDATION => EventDomain.DATA_AQUISITION
      case EventOperation.ODS_SRC_FILE_EXISTENCE_VALIDATION => EventDomain.DATA_AQUISITION
      case EventOperation.ODS_NORMALIZATION => EventDomain.DATA_AQUISITION
      case EventOperation.ODS_BUSINESS_RULE_VALIDATION => EventDomain.DATA_AQUISITION
      case EventOperation.ODS_SCHEMA_FORMATTING => EventDomain.DATA_AQUISITION
      case EventOperation.ODS_FILE_READ => EventDomain.DATA_AQUISITION
      case EventOperation.ODS_PRODUCT_BRIDGING => EventDomain.BRIDGING
      case EventOperation.ODS_LOCATION_BRIDGING => EventDomain.BRIDGING
      case EventOperation.DWH_ODS_DELTA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_ODS_NORMALIZATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_RCV_DELTA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_RCV_NORMALIZATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_REBRDG_DELTA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_REBRDG_NORMALIZATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_IMPTN_DELTA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_IMPTN_NORMALIZATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_FCC_DELTA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_FCC_NORMALIZATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_RI_DELTA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_RI_NORMALIZATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BI_TEMPORAL => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_WRITE => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_IMPUTATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_PROJECTION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_PRODUCT => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_FIND_IN_PERIODS => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_METRICS_TRANS => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_METRICS_PROD => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_METRICS_LOC => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_METRICS_AGG => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_METRICS_REGION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_METRICS_COUNTRY => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_METRICS_OVERALL => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_QC1_METADATA_SIGNOFF => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_PROC_START => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_WEEKS => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_PANEL => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_QC1_STAT => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_IMP_STAT => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_GD_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_NORM => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_WRITE => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SEND_MAIL => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_FSL_OAC_RELOC => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_PRD_CYLE => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_TMPD_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_PACK_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_LOC_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_REIMBMNT_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_BNDRY_HEIR_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_PRJTN_OUT_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_FSL_OUT_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_GET_TRNS_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_PARTN_SWTCH_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SUMM_BLD_INSRT_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_PROD_CYLE_START => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_PROD_CYLE_END => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_PROD_WEEKLY_CYLE_START => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_PROD_WEEKLY_CYLE_END => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_MONTHLY_PROD_CYLE_START => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_MONTHLY_PROD_CYLE_END => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_SIGN_OFF_START => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_WEEKLY_SIGN_OFF_START => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_MONTHLY_SIGN_OFF_START => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_PROD_CYLE_END_LOAD => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_FILE_READ => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_NORMALIZATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_VALIDATION => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_CC_INSRT_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_CC_ARCHIVE_FILE => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_ARCHIVE_DATA => EventDomain.DATAWAREHOUSE
      case EventOperation.DWH_BDC_APPR_TBL_LOAD => EventDomain.DATAWAREHOUSE
      case EventOperation.OAC_RLC_PROC_START => EventDomain.DATAWAREHOUSE
      case EventOperation.OAC_RLC_SOURCE => EventDomain.DATAWAREHOUSE
      case EventOperation.OAC_RLC_MAP => EventDomain.DATAWAREHOUSE
      case EventOperation.OAC_RLC_NORM => EventDomain.DATAWAREHOUSE
      case EventOperation.OAC_RLC_WRITE => EventDomain.DATAWAREHOUSE
      case EventOperation.OAC_RLC_SEND_MAIL => EventDomain.DATAWAREHOUSE
      case EventOperation.REF_PRD_PROC_START => EventDomain.REFERENCE_DATA
      case EventOperation.REF_PRD_READ_FTP => EventDomain.REFERENCE_DATA
      case EventOperation.REF_PRD_NORM => EventDomain.REFERENCE_DATA
      case EventOperation.REF_PRD_WRITE => EventDomain.REFERENCE_DATA
      case EventOperation.REF_PRD_SEND_MAIL => EventDomain.REFERENCE_DATA
      case _ => UNDEFINED
    }

  }

}

