package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{BemConf, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service.{AbstractServiceTrait, SparkServiceTrait}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.exception.NonCriticalException
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.dwh.ReadLmsProcessor
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.dwh.dwhProcessor.{ReadLMSProductProcessor, ReadLmsCompanyProcessor, ReadLmsDrugNamesProcessor, ReadLmsPackProcessor}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.EmailAlerts

/**
	* Created by Brahmdev Kumar on 21/08/2020.
	*
	* The input arguments to trigger the spark function
	*
	* @param lmsReadLocation - FTP directory to read information from ('approved' or 'test')
	*/
case class ReadLmsFromFtpArgs(
															 lmsReadLocation: String = ""
														 )

object ReadLmsFromFtp extends ReadLmsFromFtpTrait {
}

trait ReadLmsFromFtpTrait extends AbstractServiceTrait with SparkServiceTrait {
	val emailAlerts = new EmailAlerts(appContext, sparkExecutor)

	def actionOnEventFailed(e: Exception, correlationUUID: String, procFileName: String, fileCreTs: TimestampConverter, procName: String, shouldTerminate: Boolean): Unit = {

		/*appContext.bemLogger.logBusinessEventFinish(
			eventDescription = s"Operation failed with error: ${e.getMessage}",
			eventOutcome = BemConf.EventOutcome.FAILURE
		)*/

		val msg = s"Correlation ID: $correlationUUID,  FileName : $procFileName, File Create Date: ${fileCreTs.notTsUTCText("yyyyMMdd")}\n Operation failed with error: ${e.getMessage}"
		emailAlerts.sendStatusEmail(msg, new TimestampConverter().notTsUTCText("yyyyMMdd"), procName)
		if (shouldTerminate) throw e
	}

	def processOptions(opts: ReadLmsFromFtpArgs): Unit = {

		val sftpHost = appContext.config.getString("LMS_SFTP_HOST").trim
		val sftpPort = appContext.config.getInt("LMS_SFTP_PORT")
		val sftpUser = appContext.config.getString("LMS_SFTP_USER").trim
		val pathVal = {
			val hdfsUserPath = appContext.config.getString("HDFS_USER_PATH").trim
			val pwdFile = appContext.config.getString("LMS_SFTP_PASSWORD").trim
			s"$hdfsUserPath/$pwdFile"
		}
		val sftpPassword = sparkExecutor.file.getPwd(pathVal).trim
		val srcSftpDir = appContext.config.getString("LMS_SFTP_SRCDIR").trim + "/" + opts.lmsReadLocation
		val sftpExecutor = appContext.getSftpExecutor( sftpHost, sftpUser, sftpPassword, sftpPort)
		var emailMessage = s"Product reference file load status,\n\n"

		//1. list files
		val fileList = sftpExecutor.listFileWithCreTs(srcSftpDir, "*.txt")

    //

		//2. List processor
		val preProcessors: List[ReadLmsProcessor] = List(
			/*new ReadLMSProductProcessor(appContext, sparkExecutor, sftpExecutor),
			new ReadLmsPackProcessor(appContext, sparkExecutor, sftpExecutor),
			new ReadLmsCompanyProcessor(appContext, sparkExecutor, sftpExecutor),
			new ReadLmsDrugNamesProcessor(appContext, sparkExecutor, sftpExecutor)*/
		)

		//3. process incoming data
		fileList.foreach(file => {
			val fileName = file._1
			val fileCreTs = file._2
			var procFileType = "unknown"
			val correlationUUID = java.util.UUID.randomUUID().toString.trim.toUpperCase
			try {

				//Setting Bem Logger context for DWH Process
		/*		appContext.bemLogger.setBemLoggerContext(
					correlationUUID = correlationUUID,
					dataName = s"${ProcessDwhStreamConstants.SRC_SYS_CD_LMS_PROC}",
					eventScope = s"${ProcessDwhStreamConstants.SRC_SYS_CD_LMS_PROC}",
					productionCycle = fileCreTs.notTsUTCText("yyyyMMdd")
				)*/

				preProcessors.filter(_.isProcessing(fileName))
					.foreach(lmsProcessor => {
						//Process file
						val processingResult = lmsProcessor.process(correlationUUID, srcSftpDir, fileName, fileCreTs, emailAlerts)
						emailMessage += s"\nCorrelation ID: $correlationUUID  FileName : $fileName, File Create Date:sb, File Load Date: ${new TimestampConverter().notTsUTCText("yyyyMMdd")},  Inserted Record Count: ${processingResult._1}, Status : ${if (processingResult._2) "Pass" else "Fail"} \n";

					})

			} catch {
				case e: NonCriticalException => actionOnEventFailed(e, correlationUUID, fileName, fileCreTs, ProcessDwhStreamConstants.SRC_SYS_CD_LMS_PROC, shouldTerminate = false)
				case e: Exception => actionOnEventFailed(e, correlationUUID, fileName, fileCreTs, ProcessDwhStreamConstants.SRC_SYS_CD_LMS_PROC, shouldTerminate = true)
			}
		})

		//Send success email
   /* appContext.bemLogger.logBusinessEventStart(
      eventDescription = s"Sending Status email for ${ProcessDwhStreamConstants.SRC_SYS_CD_LMS_PROC}",
      eventOperation = BemConf.EventOperation.REF_PRD_SEND_MAIL,
      componentIdentifier = s"${ProcessDwhStreamConstants.SRC_SYS_CD_LMS_PROC}"
    )*/

		emailAlerts.sendStatusEmail(emailMessage, srcSftpDir.substring(srcSftpDir.lastIndexOf("/")+1, srcSftpDir.length), ProcessDwhStreamConstants.SRC_SYS_CD_LMS_PROC)

    /*appContext.bemLogger.logBusinessEventFinish(
      eventDescription =
        s"Status email sent for ${ProcessDwhStreamConstants.SRC_SYS_CD_LMS_PROC}",
      eventOutcome = BemConf.EventOutcome.SUCCESS
    )*/

  }

	def main(args: Array[String]): Unit = {
		//appContext.logger.module(this.getClass.getName)

		val opts = ReadLmsFromFtpArgs()
		val argParser = new scopt.OptionParser[ReadLmsFromFtpArgs]("ndh") {
			head(s"dk9-ndh", s"${getClass.getPackage.getImplementationVersion}-${getClass.getPackage.getSpecificationVersion}")
			help("help").text("Prints this usage text.")
			opt[String]('l', "lmsReadLocation")
				.action((x, c) => c.copy(lmsReadLocation = x))
				.text("Product Reference FTP Location")
				.required()
		}

		argParser.parse(args, opts) match {
			case Some(parsedOpts) =>
				processOptions(parsedOpts)
			case _ =>
				throw new IllegalArgumentException("Incorrect or no input arguments passed. Try --help to get usage text.")
		}
	}
}
