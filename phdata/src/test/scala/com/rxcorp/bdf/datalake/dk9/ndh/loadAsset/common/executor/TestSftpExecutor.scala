package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.AppLogger

class TestSftpExecutor( sftpHost:String, sftpUser:String, sftpPassword:String, sftpPortNo:Int) extends SftpExecutor( sftpHost, sftpUser, sftpPassword, sftpPortNo) {
   val _sftpExecutor=  new SftpExecutor( sftpHost, sftpUser, sftpPassword, sftpPortNo)
}
