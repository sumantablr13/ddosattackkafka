package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import scalaj.http.{Http, HttpOptions, HttpResponse}

class RestfulServiceExecutor() {

  private var jwtToken : String = null

  // Convert to JSON object
  def getJSON(businessEvent: Map[String, String]): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    write(businessEvent)
  }

  //get Access Token, return Authorization token
  def getAccessToken(businessEvent: Map[String, String], httpAuthUrl: String):Boolean = {
    resetAccessToken()
    val credentialJson: String = getJSON(businessEvent)
    val response = httpPost(httpAuthUrl, credentialJson)
    if(!response.isClientError){
      jwtToken = response
        .header("Authorization")
        .get
      true
    }
    else
      false
  }

  def resetAccessToken(): Boolean ={
    jwtToken = null
    true
  }

  //Rest API http post request
  def httpPost(httpPostUrl: String, reqJson: String, timeout: Int=20000): HttpResponse[String] ={
    Http(httpPostUrl).postData(reqJson)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(timeout))
      .asString
  }

  //Rest API http get requests
  def httpGet(httpGetUrl: String, timeout: Int=20000): HttpResponse[String] ={
    Http(httpGetUrl)
      .header("Authorization", jwtToken)
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(timeout))
      .asString
  }

}

