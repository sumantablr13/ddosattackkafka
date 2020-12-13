package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.fileStructure
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.language.existentials
import scala.reflect.runtime.universe._



class FileExecutor(spark: SparkSession) extends Executor {
  override val executorName: String = "File Executor"

  lazy val hadoopConf: Configuration = new Configuration()
  lazy val hadoopFS: FileSystem = FileSystem.get(hadoopConf)

  import spark.implicits._

  def encoderTest[T <: Product : TypeTag](ds: Dataset[Row]): Dataset[T] = {
   // logger.logMessage(s"Registering dataset as type ${Encoders.product[T].clsTag.runtimeClass}")
    ds.as[T]
  }


  def readFromHdfs(hdfsPath:String, fileName:String): Seq[String] = {
   // logger.logMessage(s"Reading HDFS file: $hdfsPath/$fileName")

    val df = spark.read.textFile(s"$hdfsPath/$fileName")
    df.collect.toSeq
  }
  def csvFileRead(path:String): DataFrame =
  {
     val df= spark.read.format("csv")
      .option("inferSchema", "true")
       .option("delimiter", ",")
      .option("header", "true")
      .load(path)
    df
  }
  def csvFileRead1(path:String): DataFrame =
  {
    val df= spark.read.format("csv")
      .load(path+"\\*.csv")
    df
  }

  def convertToDataset(df:DataFrame): Dataset[fileStructure] =
  {

   return df.as[fileStructure]
  }
  def csvFileSave(df:DataFrame,path:String): Unit =
  {
    df.coalesce(1).write
    .mode("overwrite")
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save(path)
  }


  def convertJsonToDF(jsonString: Seq[String] ): DataFrame ={
    spark.sqlContext.read.json(jsonString.toDS())
  }



  def getRdd(jsonString: Seq[com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.ComputerAttack] ): RDD[com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.ComputerAttack] ={
    spark.sparkContext.parallelize(jsonString)
  }

  def getDataFromFille(path:String,delimiter:String ) ={
    spark.read.options(Map("inferSchema"->"false","delimiter"->delimiter,"header"->"false")).csv(path)
    //spark.sparkContext.textFile(path)
   // spark.sparkContext.textFile(path)
  }



}
