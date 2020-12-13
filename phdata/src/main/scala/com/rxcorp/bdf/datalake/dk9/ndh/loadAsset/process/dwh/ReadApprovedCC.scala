package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.process.dwh

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppContext
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor.SparkExecutor
import org.apache.spark.sql.DataFrame

case class ReadApprovedCC(appContext: AppContext, sparkExecutor: SparkExecutor) {

  private val rulesNodeName = "rules"
}

  /**
    * This method return card level correction card information from rest Api. Returns single row
    *
    * @return
    */

/*def getCardLevelInfo(correctionType: String, cardSourceId: String, cardApprovalStatus: String = ""): DataFrame = {

val httpAuthUrl = appContext.config.getString("BDC_JWT_REST_URL").trim
 val httpDataUrl = s"${appContext.config.getString("BDC_DATA_REST_URL").trim}/$correctionType/$cardSourceId"
 val restCallTimeout = appContext.config.getString("REST_CALL_TIMEOUT").toInt
 val username = appContext.config.getString("ENV_USER").trim
 val pathVal = {
   val hdfsUserPath = appContext.config.getString("HDFS_USER_PATH").trim
   val pwdFile = appContext.config.getString("REST_API_PASSWORD").trim
   s"$hdfsUserPath/$pwdFile"
 }
 val password = sparkExecutor.file.getPwd(pathVal).trim
 val jsonBusinessEvent1 = Map(
   "username" -> username,
   "password" -> password
 )

 val httpService = appContext.restfulServiceExecutor
 val tokenStatus = httpService.getAccessToken(jsonBusinessEvent1, httpAuthUrl)
 if (!tokenStatus)
   throw new Exception(s"Ldap authentication failure.Unable to retrieve AWT token.")

 val approvedCC = httpService.httpGet(httpDataUrl, restCallTimeout)
 if (approvedCC.isClientError)
   throw new Exception(s"${approvedCC.body.toString}")

 //get json root content
 val cardLevelInfoDf = sparkExecutor.file.convertJsonToDF(Seq(approvedCC.body))

 if (cardApprovalStatus == "")
   cardLevelInfoDf
 else
   cardLevelInfoDf.filter(s"status = '${cardApprovalStatus.toUpperCase}'")
}

/**
 * This method return rules level correction card information from rest Api. returns mutliline.
 *
 * @return
 */

def getRuleLevelInfo(correctionType: String, cardSourceId: String, cardApprovalStatus: String = "", ruleApprovalStatus: String = ""): DataFrame = {

 val cardLevelInfoDf = getCardLevelInfo(correctionType, cardSourceId, cardApprovalStatus)

 //Get rules element array content from json
 val selectCols = Array(s"$rulesNodeName.*")
 val ruleLevelInforDf = sparkExecutor.file.getDatasetColumnArrayToRow(cardLevelInfoDf, selectCols, rulesNodeName)

 val CorrectionCardDf = cardLevelInfoDf
   .crossJoin(ruleLevelInforDf)
   .select(
     cardLevelInfoDf("cardSourceId"),
     cardLevelInfoDf("cardType"),
     //cardLevelInfoDf("createdBy"),
     //cardLevelInfoDf("createdTs"),
     cardLevelInfoDf("status").as("CardStatus"),
     //ruleLevelInforDf("effectiveTs"),
     //ruleLevelInforDf("expiryTs"),
     ruleLevelInforDf("ruleSeqNr"),
     ruleLevelInforDf("status").as("RuleStatus")
     //ruleLevelInforDf("supplierId"),
     //ruleLevelInforDf("ticketNr")
   )

 if (cardApprovalStatus == "" && ruleApprovalStatus == "")
   CorrectionCardDf
 else if (cardApprovalStatus != "" && ruleApprovalStatus == "")
   CorrectionCardDf
     .filter(s"CardStatus = '${cardApprovalStatus.toUpperCase}'")
 else if (cardApprovalStatus == "" && ruleApprovalStatus != "")
   CorrectionCardDf
     .filter(s"RuleStatus = '${ruleApprovalStatus.toUpperCase}'")
 else
   CorrectionCardDf
     .filter(s"CardStatus = '${cardApprovalStatus.toUpperCase}' AND RuleStatus = '${ruleApprovalStatus.toUpperCase}'")

}
}*/


