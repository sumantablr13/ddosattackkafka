package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.Charset
import java.sql.Timestamp
import java.time.Instant

import com.jcraft.jsch._
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{ TimestampConverter}

import scala.io.Source

class SftpExecutor( sftpHost: String, sftpUser: String, sftpPassword: String, sftpPortNo: Int) extends Executor {

  override val executorName: String = "Sftp Executor"
  private var sftpConnection: ChannelSftp = openConnection()

  private def openConnection(): ChannelSftp = {
   // logger.logMessage(s"Opening sftp channel for host $sftpHost, user name : $sftpUser")
    val jsch = new JSch()
    val session = jsch.getSession(sftpUser, sftpHost, sftpPortNo)
    session.setConfig("StrictHostKeyChecking", "no")
    session.setConfig("PreferredAuthentications", "password")
    session.setPassword(sftpPassword)
    session.connect()
    val channel: Channel = session.openChannel("sftp")
    channel.connect()
    val sftpChannel: ChannelSftp = channel.asInstanceOf[ChannelSftp]
    sftpChannel
  }

  def closeConnection(): Boolean = {
    if (sftpConnection.isClosed)
      return true
    val sftpSession = sftpConnection.getSession()
    sftpConnection.disconnect()
    sftpSession.disconnect()
    true
  }

  def reconnect(): Unit = {
    closeConnection()
    sftpConnection = openConnection()
  }
  /**
    * Method definition to check existence of Source File in sftp and return list of available files.
    *
    * @return
    */

  def checkFile(path: String, fileName: String): Boolean = {
   // logger.logMessage(s"Checking existence of the file in SFTP ${path}/${fileName} ")
    try {
      if (sftpConnection.ls(s"${path}/${fileName}").isEmpty) false else true
    }
    catch{
      case e: SftpException =>{
        if(e.id.equals(ChannelSftp.SSH_FX_NO_SUCH_FILE))
          false
        else throw e
      }
    }
  }

  /**
    * Method definition to list available files from sftp.
    *
    * @return
    */
  /**
    * Method definition to list available files from sftp with attrs.
    */
  def listFileWithCreTs(path: String, filePattern: String = "*" ) = {
   // logger.logMessage(s"Listing files from SFTP for files with pattern ${path}/${filePattern} ")
    var fileList = Seq.empty[(String,TimestampConverter)]
    var lsFile: java.util.Vector[_] = new java.util.Vector()

    try {
      lsFile = sftpConnection.ls(s"${path}/${filePattern}")
    } catch {
      case exc: SftpException =>
        if (exc.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
         // logger.logMessage(exc.getStackTrace.toString)
        } else {
          throw exc
        }
      case exc: Exception =>
        throw exc
    }

    if (lsFile.isEmpty) {
     // logger.logMessage(s"No files available for files with pattern ${path}/${filePattern}. returned empty list")
      fileList
    }
    else {
      val data = lsFile.asInstanceOf[java.util.Vector[ChannelSftp#LsEntry]].iterator()
      while (data.hasNext) {
        val file = data.next()
        val fileCreTs = new TimestampConverter(Timestamp.from(Instant.ofEpochMilli((file.getAttrs.getMTime)*1000L)))
        fileList = fileList :+ ((file.getFilename, fileCreTs))
      }
     // logger.logMessage(s"Returned files list  ${path}/${filePattern}.Available files are ${fileList}")
      fileList
    }
  }

  def listFile(path: String, filePattern: String = "*" ): Seq[String] = {
    listFileWithCreTs(path, filePattern).map(t => t._1)
  }

  /**
    * Method definition to move file form Source location to other location.
    *
    * @return
    */
  def moveFile(srcPath: String, srcFileName: String, destPath: String, destFileName: String = "") : Boolean = {
   // logger.logMessage(s"Moving file ${srcPath}/${srcFileName} to ${destPath}/${destFileName} location...")
    val newFileName =
      if (destFileName.isEmpty) srcFileName
      else destFileName


    sftpConnection.rename(s"${srcPath}/${srcFileName}", s"${destPath}/${newFileName}")
    //logger.logMessage(s"File ${srcPath}/${srcFileName} is moved to ${srcPath}/${newFileName} location..")
    true
  }

  /**
    * Method definition to move file form Source location to other location.
    *
    * @return
    */
  def deleteFile(path: String, fileName: String): Boolean = {
    //logger.logMessage(s"Deleting file ${path}/${fileName} from sftp")
    sftpConnection.rm(s"${path}/${fileName}")
    //logger.logMessage(s"File ${path}/${fileName} deleted sucssessfully..")
    true
  }

  /**
    * Method definition to read File from SFTP, returns inputStream
    */
  def readFileAsStream(path: String, fileName: String): InputStream = {
    //logger.logMessage(s"Reading file ${path}/${fileName} from sftp")
    val inpStream: InputStream =sftpConnection.get(path + "/" + fileName)
   // logger.logMessage(s"Returned file ${path}/${fileName} as InputStream from sftp..")
    inpStream
  }

  /**
   * Method definition to read File from SFTP, returns Seq
   */
  def readFileAsSeq(path: String, fileName: String, charset: Charset = Charset.forName("UTF-8")): Seq[String] = {
   // logger.logMessage(s"Reading file ${path}/${fileName} from sftp")
    val inpStream: InputStream = sftpConnection.get(path + "/" + fileName)
    val lines = Source.fromInputStream(inpStream, charset.name()).getLines().toSeq
    //logger.logMessage(s"Returned file ${path}/${fileName} as SeqString from sftp..")
    lines
  }

  /**
    * Method definition to upload File to SFTP by reading Seq as input
    */
  def uploadFile(seqString:Seq[String], destPath: String, destFileName: String): Boolean = {
    val inputStream: InputStream = new ByteArrayInputStream(seqString.mkString("\n").getBytes())
    uploadFile(inputStream: InputStream, destPath,destFileName)
  }

  /**
    * Method definition to upload File to SFTP  by reading bufferedInputStream as input
    */
  def uploadFile(inputStream: InputStream, destPath: String, destFileName: String ): Boolean = {
   // logger.logMessage(s"Uploading ${destPath}/${destFileName} to SFTP...")
    sftpConnection.put(inputStream,s"${destPath}/${destFileName}", ChannelSftp.OVERWRITE)
   // logger.logMessage(s"Upload successful for file : ${destPath}/${destFileName}")
    true
  }

  def createDirectory(path: String): Boolean = {
    //logger.logMessage(s"Creating directory in Sftp server $path to SFTP...")
    sftpConnection.mkdir(path)
    //logger.logMessage(s"Crated directory successful in sftp server: $path")
    true
  }
}

