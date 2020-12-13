package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.service.dwh

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.service.{AbstractServiceTrait, SparkServiceTrait}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.exception.NonCriticalException
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.dwh.CompressPartFiles
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.validations.EmailAlerts


/**
  * The input arguments to trigger the spark function
  *
  * (none exists at present)
  */

case class MergePartFilesArgs (
                                dbObjectName: String = ""
                                , partCols: String = ""
                              )

object MergePartFiles extends MergePartFilesTrait {
}

trait MergePartFilesTrait extends AbstractServiceTrait with SparkServiceTrait {

  val emailAlerts = new EmailAlerts(appContext, sparkExecutor)

  def actionOnEventFailed(e: Exception): Unit = {
    //appContext.logger.logError(e)
    throw e
  }

  def processOptions(opts: MergePartFilesArgs): Unit = {
    try {
      new CompressPartFiles(appContext, sparkExecutor, opts.dbObjectName, opts.partCols)
    } catch {
      case e: Exception =>
        actionOnEventFailed(e)
    }
  }

  def main(args: Array[String]): Unit = {
   // appContext.logger.module(this.getClass.getName)
    //appContext.logger.logMessage("Finished processing the data - exiting")

    val opts = MergePartFilesArgs()
    val argParser = new scopt.OptionParser[MergePartFilesArgs]("PBS") {
      head(s"gb9-pbs", s"${getClass.getPackage.getImplementationVersion}-${getClass.getPackage.getSpecificationVersion}")
      help("help").text("Prints this usage text.")

      opt[String]('o', "objectName")
        .action((x, c) => c.copy(dbObjectName = x.trim().toUpperCase()))
        .text("Should be dbName.tableName!")
        .required()

      opt[String]('c', "partCols")
        .action((x, c) => c.copy(partCols = x.trim().toUpperCase()))
        .text("Should be Columns Names with Comma separated...!")
        .optional()
    }

    argParser.parse(args, opts) match {
      case Some(dIArgs) =>
        processOptions(dIArgs)
      case None =>
        throw new NonCriticalException("Incorrect or no input arguments passed. Try --help to get usage text.")
    }
  }
}
