package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common

import java.util.{Date, Properties}

import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Address, Message, Session, Transport}


/**
  * Created by Shiddesha.Mydur on 3rd Jul 2019
  */

class MailAgent(to: String
                , cc: String
                , bcc: String
                , from: String
                , subject: String
                , content: String
                , smtpHost: String) {

  val message: Message = createMessage
  message.setFrom(new InternetAddress(from))
  setToCcBccRecipients()

  message.setSentDate(new Date())
  message.setSubject(subject)
  message.setText(content)

  // throws MessagingException
  def sendMessage(isHtml: Boolean ) {
    println(s"Message to test is ${message}")
    //println(s"Mail content is ${content}")
    if(isHtml) message.setContent(content,"text/html; charset=utf-8")
    Transport.send(message)
  }

  def createMessage: Message = {
    val properties = new Properties()
    properties.put("mail.smtp.host", smtpHost)
    properties.put("mail.transport.protocol", "smtp")
    val session = Session.getDefaultInstance(properties, null)
    //session.setDebug(true)
    new MimeMessage(session)
  }

  // throws AddressException, MessagingException
  def setToCcBccRecipients() {
    setMessageRecipients(to, Message.RecipientType.TO)

    if (cc != null)
      setMessageRecipients(cc, Message.RecipientType.CC)

    if (bcc != null)
      setMessageRecipients(bcc, Message.RecipientType.BCC)
  }

  // throws AddressException, MessagingException
  def setMessageRecipients(recipient: String, recipientType: Message.RecipientType) {
    val addressArray = InternetAddress.parse(recipient).asInstanceOf[Array[Address]]
    if ((addressArray != null) && (addressArray.length > 0))
      message.setRecipients(recipientType, addressArray)
  }
}