package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

import scala.util.Try

/**
  * Created by Shiddesha.Mydur on 3rd Jul 2019
  */

class EmailUtil(appContext: AppContext) {

  case class EmailConfig(from: String, to: String, cc: String, bcc: String, smtpHost: String,addinlList:String) {
    override def toString: String = {
      new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("from", from)
        .append("to", to)
        .append("cc", cc)
        .append("bcc", bcc)
        .append("smtpHost", smtpHost)
        .toString
    }
  }

  def getConfig(): Config = {
    val prop = new Properties()
    ConfigFactory.parseProperties(prop)
  }

  def createEmailConfig: EmailConfig = {
    val emailConf: Config = appContext.config.withFallback(getConfig())
    val from: String = emailConf.getString("email.from")
    val to: String = emailConf.getString("email.to")
    val cc: String = Try(emailConf.getString("email.cc")).getOrElse("")
    val bcc: String = Try(emailConf.getString("email.bcc")).getOrElse("")
    val smtpHost: String = emailConf.getString("email.smtpHost")
    val addinlList: String = Try(emailConf.getString("email.addinlList")).getOrElse("")
    EmailConfig(from, to, cc, bcc, smtpHost,addinlList)
  }

  val emailConf = createEmailConfig
  val from: String = emailConf.from
  val smtpHost: String = emailConf.smtpHost
  val addinlList: String = emailConf.addinlList
  var to: String = emailConf.to
  var cc: String = emailConf.cc
  var bcc: String = emailConf.bcc
  var subject: Option[String] = Option.empty
  var content: Option[String] = Option.empty

  def setSubject(sub: String): Unit = {
    this.subject = Some(sub)
  }

  def setContent(content: String): Unit = {
    this.content = Some(content)
  }

  def sendMail(isHtmlContent: Boolean = false,ismailAddinlList: Boolean = false ): Unit = {

    //This is to include addional DL list for specific process open/close cycle & BDE
    if (ismailAddinlList==true){
      to = to.concat(addinlList)
    }

    val mail: MailAgent = new MailAgent(to,
      cc,
      bcc,
      from,
      subject.get,
      content.get,
      smtpHost)

    mail.sendMessage( isHtmlContent )
  }
}