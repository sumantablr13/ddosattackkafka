package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor


class TestSftpExecutor( sftpHost:String, sftpUser:String, sftpPassword:String, sftpPortNo:Int) extends SftpExecutor( sftpHost, sftpUser, sftpPassword, sftpPortNo) {
   val _sftpExecutor=  new SftpExecutor( sftpHost, sftpUser, sftpPassword, sftpPortNo)
}
