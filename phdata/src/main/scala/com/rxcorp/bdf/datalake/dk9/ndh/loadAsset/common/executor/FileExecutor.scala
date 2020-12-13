package com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.executor

import java.io._
import java.security.MessageDigest

import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.exception.ParseException
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.common.{AppContext, AppLogger, TimestampConverter}
import com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.processor.ods.SupplierRecordsFile
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

import scala.io.Source
import scala.language.existentials
import scala.reflect.runtime.universe._
import scala.tools.nsc.interpreter.InputStream
import scala.util.{Failure, Success, Try}


class FileExecutor(spark: SparkSession) extends Executor {
  override val executorName: String = "File Executor"

  lazy val hadoopConf: Configuration = new Configuration()
  lazy val hadoopFS: FileSystem = FileSystem.get(hadoopConf)

  import spark.implicits._

  def encoderTest[T <: Product : TypeTag](ds: Dataset[Row]): Dataset[T] = {
   // logger.logMessage(s"Registering dataset as type ${Encoders.product[T].clsTag.runtimeClass}")
    ds.as[T]
  }
  def readJsonAsDataFrame(json: String, schema: StructType = null): DataFrame = {
    val reader = spark.read
    Option(schema).foreach(reader.schema)
    reader.json(spark.sparkContext.parallelize(Array(json)))
  }
  def encoderForList[T <: Product : TypeTag](list: java.util.List[T]): Dataset[T] = {
   // logger.logMessage(s"Registering list as type ${Encoders.product[T].clsTag.runtimeClass}")
    spark.createDataset(list)
  }

  def fixedWidthDataset[T <: Product : TypeTag](file: String, columnSizes: Array[Int]): (Dataset[T], Dataset[String]) = {
   // logger.logMessage(s"Reading fixed width file: $file")
    //val encoder = Encoders.product[T]
    val encoder = Encoders.product
    val addRawLine = encoder.schema.fields.exists(f => f.name == "rawLine")

    Option(fixedWidthDatasetFromFile[T](file, columnSizes, addRawLine))
      .getOrElse(fixedWidthDatasetFromInputStream(this
        .getClass
        .getResourceAsStream(file), columnSizes, addRawLine))
  }

  def fixedWidthDatasetFromFileWithRecType(file: String, recTypeDets: Array[String],
                                           addRawLine: Boolean,hasHeader:Boolean,
                                           isEncoded:Boolean,noRecType:Boolean
                                           ,multipleHAndTRecs:Boolean): (Array[Dataset[String]], Long) = {
   // logger.logMessage(s"Reading fixed width file with record type: $file")
    var rawFile1=spark.emptyDataset[String]
    rawFile1 = spark.read.textFile(file)

    if(isEncoded){
      rawFile1=spark.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](file)
        .mapPartitions(_.map(line=>new String(line._2.getBytes,0,line._2.getLength,"windows-1252"))).toDS()
    }else {
      rawFile1 = spark.read.textFile(file)
    }

    if(hasHeader){
      val header=rawFile1.first()
      rawFile1=rawFile1.rdd.filter(row=>row!=header).toDS()
    }
    if(multipleHAndTRecs){
      rawFile1=rawFile1.map(line=>{
        if(line.startsWith("\u001aH")){
          line.substring(1)
        }else{
          line
        }
      })
    }
    val rawFile = rawFile1.rdd.zipWithIndex.map(x =>  x._1 + (x._2 + 1).toString.reverse.padTo(10, "0").mkString.reverse).toDS()

    val allDataSplitByRecType = recTypeDets.map(recType => rawFile.filter(line =>
      line.startsWith(recType)))
    var totalRecordsInFile:Long=0
    if(noRecType){
      totalRecordsInFile=rawFile.count()
    }else {
      totalRecordsInFile = allDataSplitByRecType.reduceLeft((x: Dataset[String], y: Dataset[String]) => x.union(y))
        .count()
    }

    (allDataSplitByRecType, totalRecordsInFile)
  }

  def fixedWidthDatasetFromDS[T <: Product : TypeTag](ds: Dataset[String], dsDescription: String, columnSizes: Array[Int], addRawLine: Boolean): (Dataset[T], Dataset[String]) = {
    val (good, bad) = fixedWidthDataFrameFromDS[T](ds, dsDescription, columnSizes, addRawLine)
    (good.as[T], bad)
  }


  def fixedWidthDataFrameFromDS[T <: Product : TypeTag](ds: Dataset[String], dsDescription: String, columnSizes: Array[Int],
                                                        addRawLine: Boolean,
                                                        badRecValidation:Boolean = false,
                                                        maxLength:Int = 0): (DataFrame, Dataset[String]) = {
   // logger.logMessage(s"Parsing fixed width dataframe: $dsDescription")

    val (good, bad) = if (badRecValidation)
      ds.findGoodRecsFromBadDataSet(ds,columnSizes,ScalaReflection.schemaFor[T]
        .dataType
        .asInstanceOf[StructType],maxLength)
    else ds
      .fixedWidth(
        columnSizes,
        ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType],
        addRawLine
      )

    (good, bad)
  }

  def removeHeader(ds: Dataset[String]): Dataset[String] ={
    val rdd = ds.rdd.mapPartitionsWithIndex{
      case (index, iterator) => if (index == 0) iterator.drop(1) else iterator
    }
    spark.createDataset[String](rdd)
  }

  def readTextFile(file: String, hasHeader: Boolean): Dataset[String] = {
    val ret = spark
      .read
      .textFile(file)

    if (hasHeader)
      return removeHeader(ret)

    ret
  }

  private def readCsvAsDatasetPrivate[T <: Product : TypeTag](input: Any, delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T] = {
    val readCsvDfr = spark
      .read
      .option("header", hasHeader)
      .option("delimiter", delimiter)
      .option("timestampFormat", timestampFormat)
      .option("quote", quote)
      .option("mode", "FAILFAST")
      .option("ignoreLeadingWhiteSpace", trimWhiteSpaces)
      .option("ignoreTrailingWhiteSpace", trimWhiteSpaces)
      .schema(Encoders.product[T].schema)

    (input match {
      case (df: Dataset[_]) =>
       // logger.logMessage(s"Parsing dataset of strings as CSV")
        readCsvDfr
          .csv(df.as[String])
      case (file: String) =>
       // logger.logMessage(s"Parsing file as CSV")
        readCsvDfr
          .csv(file)
    }).as[T]
  }

  def readCsvAsDataset[T <: Product : TypeTag](df: Dataset[String], delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T] = {
    readCsvAsDatasetPrivate(df, delimiter, hasHeader, timestampFormat, trimWhiteSpaces, quote)
  }

  def readCsvAsDataset[T <: Product : TypeTag](file: String, delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T] = {
    readCsvAsDatasetPrivate(file, delimiter, hasHeader, timestampFormat, trimWhiteSpaces, quote)
  }

  def readCsvAsDataset[T <: Product : TypeTag](inputStream: InputStream, delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T]= {
    //logger.logMessage(s"Parsing input stream as CSV")
    val lines = Source.fromInputStream(inputStream).getLines().toSeq
    readCsvAsDataset[T](lines, delimiter, hasHeader, timestampFormat, trimWhiteSpaces, quote)
  }

  def readCsvAsDataset[T <: Product : TypeTag](seqString: Seq[String], delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T]= {
    //logger.logMessage(s"Parsing sequence of strings as CSV")
    val linesDS: Dataset[String] = spark.createDataset[String](seqString)(Encoders.STRING)
    readCsvAsDataset[T](linesDS, delimiter, hasHeader, timestampFormat, trimWhiteSpaces, quote)
  }

  private def readCsvAsDatasetCustomPrivate[T <: Product : TypeTag](input: Any, delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T] = {
    var inputDesc = ""
    val dsString = (input match {
      case (ds: Dataset[_]) =>
        inputDesc = "dataset"
        //logger.logMessage(s"Parsing $inputDesc as CSV")
        if (hasHeader)
          removeHeader(ds.as[String])
        else ds
      case (file: String) =>
        inputDesc = file
       // logger.logMessage(s"Parsing $inputDesc as CSV")
        readTextFile(file, hasHeader)
    }).cache

    val schema = Encoders.product[T].schema
    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(schema)

    val quoteMrk = if (quote == null) "" else quote

    dsString.toDF.map(row => {
      // split on the delimiter only if that delimited has zero, or an even number of quotes ahead of it.
      val parsed = row.getString(0).split(s"\\${delimiter.charAt(0)}(?=([^$quoteMrk]*$quoteMrk[^$quoteMrk]*$quoteMrk)*[^$quoteMrk]*$$)")
        .map(value => if (quoteMrk != "") value.replaceAll(s"^$quoteMrk|$quoteMrk$$", "") else value)
        .map(value => if (trimWhiteSpaces) value.trim else value)
        .map(value => if (value == "") null else value)

      if (parsed.length != schema.length)
        throw new ParseException(s"ERROR: Failed to parse $inputDesc - found ${parsed.length} fields, expecting ${schema.length} fields in row ${row.getString(0)}")

      Row.fromSeq(
        schema
          .map(field => {
            val ret = parsed(schema.indexOf(field))
            try {
              if (ret == null) null
              else field.dataType match {
                case (_: StringType) => ret
                case (_: IntegerType) => Integer.parseInt(ret)
                case (_: DecimalType) => BigDecimal.apply(ret)
                case (_: TimestampType) => TimestampConverter.parse(ret, timestampFormat).ts
                case (x: DataType) => throw new Exception(s"Unrecognized data type: $x")
                case _ => null
              }
            } catch {
              case exc: Exception => throw new ParseException(s"ERROR: Failed to parse $inputDesc field $field (#${schema.indexOf(field)+1}) content($ret) for row ${row.getString(0)}", exc)
            }
          })
      )
    })
      .as[T]
  }

  def readCsvAsDatasetCustom[T <: Product : TypeTag](df: Dataset[String], delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T] = {
    readCsvAsDatasetCustomPrivate(df, delimiter, hasHeader, timestampFormat, trimWhiteSpaces, quote)
  }

  def readCsvAsDatasetCustom[T <: Product : TypeTag](file: String, delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T] = {
    readCsvAsDatasetCustomPrivate(file, delimiter, hasHeader, timestampFormat, trimWhiteSpaces, quote)
  }

  def readCsvAsDatasetCustom[T <: Product : TypeTag](inputStream: InputStream, delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T]= {
   // logger.logMessage(s"Parsing input stream as CSV")
    val lines = Source.fromInputStream(inputStream).getLines().toSeq
    readCsvAsDatasetCustom[T](lines, delimiter, hasHeader, timestampFormat, trimWhiteSpaces, quote)
  }

  def readCsvAsDatasetCustom[T <: Product : TypeTag](seqString: Seq[String], delimiter: String, hasHeader: Boolean, timestampFormat: String, trimWhiteSpaces: Boolean, quote: String): Dataset[T]= {
   // logger.logMessage(s"Parsing sequence of strings as CSV")
    val linesDS: Dataset[String] = spark.createDataset[String](seqString)(Encoders.STRING)
    readCsvAsDatasetCustom[T](linesDS, delimiter, hasHeader, timestampFormat, trimWhiteSpaces, quote)
  }

  def delimitedDataset[T <: Product : TypeTag]( file: String, delimiter:String ): ( Dataset[T], Dataset[String] ) = {
  //  logger.logMessage(s"Reading delimited file $file")
    val (good, bad) = spark
      .read
      .textFile(file)
      .delimited(
        ScalaReflection.schemaFor[T]
          .dataType
          .asInstanceOf[StructType], delimiter)
    (good.as[T], bad)
  }


private def fixedWidthDatasetFromInputStream[T <: Product : TypeTag](inputStream: InputStream, columnSizes: Array[Int], addRawLine: Boolean): (Dataset[T], Dataset[String]) = {
    val (good, bad) =
      spark
        .sparkContext
        .parallelize(Source
          .fromInputStream(inputStream)
          .getLines()
          .toList)
        .toDS
        .fixedWidth(columnSizes,
          ScalaReflection.schemaFor[T]
            .dataType
            .asInstanceOf[StructType], addRawLine)

    (good.as[T], bad)
  }

  def fixedWidthDatasetFromSeqString[T <: Product : TypeTag](inputSeq: Seq[String], columnSizes: Array[Int], addRawLine: Boolean): (Dataset[T], Dataset[String]) = {
    val (good, bad) =
      spark
        .sparkContext
        .parallelize(inputSeq)
        .toDS
        .fixedWidth(columnSizes,
          ScalaReflection.schemaFor[T]
            .dataType
            .asInstanceOf[StructType], addRawLine)

    (good.as[T], bad)
  }

  def fixedWidthDatasetFromSeqString[T <: Product : TypeTag](inputSeq: Seq[String], columnSizes: Array[Int], addRawLine: Boolean, isGeneric: Boolean = false): (Dataset[T], Dataset[String]) = {
    val (good, bad) = {
      if(isGeneric){
        spark
          .sparkContext
          .parallelize(inputSeq)
          .toDS
          .fixedWidth(columnSizes,
            ScalaReflection.schemaFor[T]
              .dataType
              .asInstanceOf[StructType], addRawLine, isGeneric)
      }
      else {
        spark
          .sparkContext
          .parallelize(inputSeq)
          .toDS
          .fixedWidth(columnSizes,
            ScalaReflection.schemaFor[T]
              .dataType
              .asInstanceOf[StructType], addRawLine)
      }
    }

    (good.as[T], bad)
  }

  private def fixedWidthDatasetFromFile[T <: Product : TypeTag](file: String, columnSizes: Array[Int], addRawLine: Boolean): (Dataset[T], Dataset[String]) = {
    val (good, bad) =
      spark
        .read
        .textFile(file)
        .fixedWidth(columnSizes,
          ScalaReflection.schemaFor[T]
            .dataType
            .asInstanceOf[StructType], addRawLine)

    (good.as[T], bad)
  }

  implicit class DatasetStringExt(ds: Dataset[String]) {

    import ds.sparkSession.implicits._

    def fixedWidth(columnSizes: Array[Int], schema: StructType, addRawLine: Boolean): (Dataset[Row], Dataset[String]) = {
      implicit val encoder: ExpressionEncoder[Row] = RowEncoder(schema)

      val colLength = columnSizes.sum
      val badDS = ds.filter(line => {
        !(line.length >= colLength)
      })


      val goodDS =
        ds.filter(line => line.length >= colLength)
          .map(lines => {
            var sizeStart = 0
            var indexPos=0
            val line = lines.substring(0,lines.length-10)
            val seq = columnSizes.map(size => {
              sizeStart += size
              var str=""
              if(indexPos==columnSizes.length-2) {
                str=line.substring(sizeStart - size).trim
                indexPos += 1
              }else if(indexPos < columnSizes.length-1){
                indexPos += 1
                str=line.substring(sizeStart - size, sizeStart).trim

              }else{
                str=lines.takeRight(10)
                indexPos += 1
              }
              str
            })
            if (addRawLine) {
              seq :+ line
            }
            else {
              seq
            }
          })
          .map(row => {
            Row.fromSeq(row)
          })

      (goodDS, badDS)
    }

    /**
     * This is a generic method to parse a fixed width file.
     * @param columnSizes Array[Int] which contains the size of each column.
     * @param schema schema of the fixed width file.
     * @param addRawLine boolean flag to determine whether to add a rawLine or Not.
     * @param isGeneric boolean flag to determine whether to call generic fixed length file parser or custom fixed length file parser.
     * @return
     */
    def fixedWidth(columnSizes: Array[Int], schema: StructType, addRawLine: Boolean, isGeneric: Boolean): (Dataset[Row], Dataset[String]) = {
      implicit val encoder: ExpressionEncoder[Row] = RowEncoder(schema)

      val colLength = columnSizes.sum
      val badDS = ds.filter(line => {
        !(line.length >= colLength)
      })

      val goodDS = ds.filter(line => line.length >= colLength)
        .map(lines => {
          var startSize = 0
          var indexPos = 0
          val seq = columnSizes.map(size => {
            startSize += size
            var str = ""
            str = lines.substring(indexPos, startSize).trim
            indexPos += size
            str
          })
          if (addRawLine) {
            seq :+ lines
          } else {
            seq
          }
        }).map(row => {
        Row.fromSeq(row)
      })

      (goodDS, badDS)
    }

    def delimited(schema: StructType, delimiter:String): (Dataset[Row], Dataset[String]) = {
      implicit val encoder = RowEncoder(schema)
      val df1: Dataset[Array[String]] = ds.map(line => line.split(s"\\${delimiter}", -1)).map(ln => {
        var str = Array[String]()
            var isQuotesStared = false
            var lines = ""
            for(i<-0 to ln.length-1){
              val line = ln(i)
              if(line.startsWith("\"") && line.endsWith("\"") && line!="\""){
                isQuotesStared = false
                lines = ""
                str :+= line.stripPrefix("\"").stripSuffix("\"")
              }else if(line.startsWith("\"") && isQuotesStared==false){
                isQuotesStared = true
                lines = line
              }else if(line.endsWith("\"") && isQuotesStared==true){
                isQuotesStared = false
                lines += delimiter + line
                str :+= lines.stripPrefix("\"").stripSuffix("\"")
                lines = ""
              }else{
                if(isQuotesStared){
                  lines += delimiter + line
                  lines
                }else{
                  isQuotesStared = false
                  lines = ""
                  str :+= line
                }
              }
            }
            str
          })


      val goodRecords = df1.filter(x => x.length == schema.length)
      val badRecords = df1.filter(x => x.length != schema.length)
      (goodRecords
        .map(row => {
          Row.fromSeq(row)
        }),
        badRecords.map(x => x.mkString(delimiter)))
    }

    def findGoodRecsFromBadDataSet(ds:Dataset[String], columnSizes: Array[Int], schema: StructType, maxLength:Int): (Dataset[Row], Dataset[String]) = {
      implicit val encoder: ExpressionEncoder[Row] = RowEncoder(schema)
      val badRecs = ds.filter(line =>{
        !(line.length-10 >= maxLength)
      })
      val goodRecs = ds.filter(line=> line.length-10 >= maxLength).map(lines=>{
        var sizeStart = 0
        var indexPos=0
        val line = lines.substring(0,lines.length-10)
        val seq = columnSizes.map(size => {
          sizeStart += size
          var str=""
          if(indexPos == columnSizes.length-2) {
            str=Try(line.substring(sizeStart - size).trim) match{
              case Success(trimmedLine) => trimmedLine
              case Failure(_) =>Try(line.substring(sizeStart - size).trim) match {
                case Success (validRec) => validRec
                case Failure(_) => ""
              }
            }
            indexPos += 1
          }else if(indexPos < columnSizes.length-1){
            indexPos += 1
            str=Try(line.substring(sizeStart - size, sizeStart).trim) match {
              case Success(trimmedLine) => trimmedLine
              case Failure(_) =>Try(line.substring(sizeStart - size).trim) match {
                case Success (validRec) => validRec
                case Failure(_) => ""
              }
            }

          }else{
            str=lines.takeRight(10)
            indexPos += 1
          }

          str
        })
        seq
      }).map(row=>{
        Row.fromSeq(row)
      })
      (goodRecs,badRecs)
    }


  }


  implicit class DatasetAnyExt[T](ds: Dataset[T]) {

    import ds.sparkSession.implicits._

    def joinAs[U <: Product : TypeTag](
                                        right: Dataset[_],
                                        joinExprs: Column,
                                        joinType: String = "left_outer"
                                      ): Dataset[U] = {
      ds.sparkSession.createDataFrame(
        ds.join(right, joinExprs, joinType).rdd,
        ScalaReflection.schemaFor[U]
          .dataType
          .asInstanceOf[StructType])
        .as[U]
    }

    def split(filterA: (T) => Boolean, filterB: (T) => Boolean): (Dataset[T], Dataset[T]) = {
      (ds.filter(filterA), ds.filter(filterB).cache)
    }

    // Overloaded to cache later
    def splitNoCache(filterA: (T) => Boolean, filterB: (T) => Boolean): (Dataset[T], Dataset[T]) = {
      (ds.filter(filterA), ds.filter(filterB))
    }
  }


  def getPwd(filePath: String): String = {
    /*
    val conf = new SparkConf().setAppName(appName)
    val sc = SparkContext.getOrCreate(conf)
*//*
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hadoopFS = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
*/
    def getFromfile(path: String): Try[String] = {
      Try(spark.sparkContext.textFile(path).take(1)(0))
    }

    def validatePathAndGet(fileHDFSPath: String): String = {
      if (hadoopFS.exists(new org.apache.hadoop.fs.Path(fileHDFSPath))) {
        getFromfile(fileHDFSPath) match {
          case Success(str) => str
          case Failure(failure) =>
            println("Failed.." + failure.printStackTrace)
            ""
        }
      }
      else {
        throw new FileNotFoundException(fileHDFSPath + "does not exist. hence exiting...")
      }
    }


    validatePathAndGet(filePath)

  }

  def convertStringToHdfsPath(hdfsPath: String): Path = {
    new Path(hdfsPath)
  }

  def writeRecordsToHDFS(
                          errorPath: String,
                          fileName: String,
                          errorRecord: DataFrame,
                          header: Boolean = true,
                          quotesAll: Boolean = true,
                          copyRawFile: Boolean = false): Boolean = {
   // logger.logMessage(s"File Path to write records $errorPath")
   // logger.logMessage(s"Received file name  $fileName")
    val parallelExecutionPath = new TimestampConverter().notTsUTCText("yyyyMMddHHmmssSSS")

    val tmpHdfsPath = errorPath.trim+"/"+parallelExecutionPath.trim
    hadoopFS.mkdirs(convertStringToHdfsPath(tmpHdfsPath))
    if (!hadoopFS.exists(convertStringToHdfsPath(errorPath))){
      hadoopFS.mkdirs(convertStringToHdfsPath(errorPath))
    }
    if(hadoopFS.exists(convertStringToHdfsPath(errorPath+"/"+fileName))){
      hadoopFS.rename(convertStringToHdfsPath(errorPath+"/"+fileName), new Path(tmpHdfsPath+"/"+fileName))
    }
    hadoopFS.deleteOnExit(convertStringToHdfsPath(errorPath+"/"+fileName))
    var file:FileStatus = null
    if(copyRawFile){
      errorRecord.coalesce(1).write.option("header", header).option("quoteAll", quotesAll).option("nullValue", "").mode("append").text(tmpHdfsPath)
      file = hadoopFS.globStatus(convertStringToHdfsPath(tmpHdfsPath+"/part*.txt"))(0)
    }else{
      errorRecord.coalesce(1).write.option("header", header).option("quoteAll", quotesAll).option("nullValue","").option("escape","").mode("append").csv(tmpHdfsPath)
      file = hadoopFS.globStatus(convertStringToHdfsPath(tmpHdfsPath+"/part*.csv"))(0)
    }

    val status = hadoopFS.rename(file.getPath, new Path(errorPath+"/"+fileName))
   // logger.logMessage("File Written status..." + status)
    status
  }

  def moveToHdfsDirectory(srcFile: SupplierRecordsFile, destPath: String, destFileExt: String = "", appContext:AppContext): Boolean={

    val srcFileName = srcFile.layoutNm
    val srcFileFullName = srcFile.batchTable
    val tgtFileName = srcFileName + destFileExt
    val tgtFileFullName = destPath + "/" + tgtFileName

   // logger.logMessage(s"Source file being moved: $srcFileFullName")
   // logger.logMessage(s"Target file destination: $tgtFileFullName")


    if (!hadoopFS.exists(convertStringToHdfsPath(destPath))){
      hadoopFS.mkdirs(convertStringToHdfsPath(destPath))
    }
    if (hadoopFS.exists(convertStringToHdfsPath(tgtFileFullName))) {
     // logger.logMessage(s"Target file already exists: $tgtFileFullName")
      val srcFileExtension = srcFileName.split("\\.").last
      val destFileExtension = if (destFileExt.isEmpty) "." + srcFileExtension else destFileExt
      val fileSuffix = new TimestampConverter().notTsUTCText("yyyyMMddHHmmssSSS") + destFileExtension
      val tgtFileFullNameExisting = destPath + "/" + srcFileName + "." + fileSuffix
      val status = hadoopFS.rename(convertStringToHdfsPath(tgtFileFullName), convertStringToHdfsPath(tgtFileFullNameExisting))
      //hadoopFS.deleteOnExit(convertStringToHdfsPath(tgtFileFullNameForExisting))  //alternatively we can also delete
     // logger.logMessage(s"Already existing target file renamed to: $tgtFileFullNameExisting")
     // logger.logMessage("Already existing target file status: " + status)
    }

    val status = hadoopFS.rename(convertStringToHdfsPath(srcFileFullName), convertStringToHdfsPath(tgtFileFullName))
    //logger.logMessage("Target file write status: " + status)

    status
  }

  def moveToRejectDirectory(file:SupplierRecordsFile, errorPath: String, appContext:AppContext): Boolean = {
    moveToHdfsDirectory(file, errorPath, ".rej", appContext)
  }

  def moveToArchiveDirectory(file:SupplierRecordsFile, archPath: String, appContext:AppContext): Boolean = {
    moveToHdfsDirectory(file, archPath, "", appContext)
  }

  def isFileExists(filePath:String):Boolean= {
    val path=convertStringToHdfsPath(filePath)
    hadoopFS.exists(path)
  }

  def isFileValid(filePath:String):(Boolean,Long)={
    val path=convertStringToHdfsPath(filePath)
    val fileSize=hadoopFS.getContentSummary(path).getLength
    if(fileSize ==0){
      (true,fileSize)
    }else{
      (false,fileSize)
    }
  }

  def getLineCount(filePath:Path):Long={
    var count=0
    val inputStream:FSDataInputStream=hadoopFS.open(filePath)
    val reader:BufferedReader=new BufferedReader(new InputStreamReader(inputStream))
    var lines=reader.readLine()
    while(lines!=null && count <4){
      lines=reader.readLine()
      count+=1
    }
    reader.close()
    inputStream.close()
    count
  }

  def writeToHdfs(inputStream: InputStream, path: String, fileName: String): Boolean = {
    val lines = Source.fromInputStream(inputStream).getLines().toSeq
    writeToHdfs(lines, path, fileName)
  }

  def writeToHdfs(seqString: Seq[String], path: String, fileName: String): Boolean = {
   // logger.logMessage(s"Storing data to $path/$fileName")

    val linesDS = spark.createDataset[String](seqString)(Encoders.STRING)
    linesDS.write.mode(SaveMode.Overwrite).text(s"$path/$fileName")
    true
  }

  def readFromHdfs(hdfsPath:String, fileName:String): Seq[String] = {
   // logger.logMessage(s"Reading HDFS file: $hdfsPath/$fileName")

    val df = spark.read.textFile(s"$hdfsPath/$fileName")
    df.collect.toSeq
  }

  /**
    * This code will genereate MD5 for the file content.
    * */
  def generateMd5ForFile(event:SupplierRecordsFile):String={
    val stream = hadoopFS.open(new org.apache.hadoop.fs.Path(event.tabToValidate))
    val arr = IOUtils.toByteArray(stream)
    stream.close()
    val checksum = MessageDigest.getInstance("MD5") digest arr
    checksum.map("%02X" format _).mkString
  }

  /**
    * Method to create for unique UUID
    * @return
    */
  def uuid: String = java.util.UUID.randomUUID.toString

  /**
    * Added by Shiddesha.Mydur on 10/07/2019
    * Defined this method to read a file with specific delimiter and return as Dataset of string...
    * @param file
    * @param delimiter
    * @return
    */

  //  def readDelimitedFileAsStringDataset( file: String
  //                                        , delimiter: String
  //                                        , hasHeader: Boolean = false
  //                                        , timestampFormat: String = "YYYY-MM-DD HH:MM:SS"
  //                                        , trimWhiteSpaces: Boolean = false
  //                                        , quoteAll: Boolean = false
  //                                        , dummy: String = "null"
  //                                        , escape: String = ""
  //                                      ): Dataset[String] = {
  //    import spark.implicits._
  //    spark
  //      .read
  //      .option("header", hasHeader)
  //      .option("delimiter", delimiter)
  //      .option("quoteAll", false)
  //      .option("escape", escape)
  //      .option("mode", "FAILFAST")
  //      .option("ignoreLeadingWhiteSpace", trimWhiteSpaces)
  //      .option("ignoreTrailingWhiteSpace", trimWhiteSpaces)
  //      .textFile(file)
  //      .rdd.zipWithIndex
  //      .map( x => ( x._1 ) + delimiter + dummy )
  //      .toDS()
  //  }


  def readDelimitedFileAsStringDataset1(file:String, delimiter:String, quote: String = "\"", nullValue: String = null, hasHeader: Boolean = false, quoteAll: Boolean = false ): Dataset[String]= {
    spark
      .read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("quoteAll", true)
      .option("quote", quote)
      .option("escape", quote)
      .option("nullValue", nullValue)
      .option("header", hasHeader)
      .option("delimiter", delimiter)
      .option("mode", "PERMISSIVE")
      .csv(file)
      .rdd.zipWithIndex
      .map(x => x._1.mkString + delimiter + (x._2 + 1).toString.reverse.padTo(10, "0").mkString.reverse).toDS()
  }

  def readDelimitedFileAsStringDataset(file: String, delimiter: String )= {
    spark
      .read
      .textFile(file)
      .rdd
      .zipWithIndex
      .map(x => x._1 + delimiter + (x._2 + 1).toString.reverse.padTo(10, "0").mkString.reverse).toDS()
  }

  /**
    * Added by shiddesha.mydur on 10/07/2019
    * @param ds

    * @tparam T
    * @return
    */
  def readDelimitedDatasetFromStringDataset[T <: Product : TypeTag]( ds: Dataset[String], delimiter: String): (Dataset[T], Dataset[String], Long) = {
    import org.apache.spark.sql.types._
    import spark.implicits._
    val (good, bad) = ds.delimited(
      ScalaReflection.schemaFor[T]
        .dataType
        .asInstanceOf[StructType], delimiter)
    val totalRecordsInFile = ds.count()
    (good.as[T], bad, totalRecordsInFile)
  }


  /**
    * Added by Brahmdev Kumar  on
    * This methods converts json string into dataframe
    * @return DataFrame
    */
  def convertJsonToDF(jsonString: Seq[String] ): DataFrame ={
    spark.sqlContext.read.json(jsonString.toDS())
  }

  def getRdd(jsonString: Seq[com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.ComputerAttack] ): RDD[com.rxcorp.bdf.datalake.dk9.ndh.loadAsset.model.daqe.logical.ComputerAttack] ={
    spark.sparkContext.parallelize(jsonString)
  }

  def getDataFromFille(path:String ) ={
    spark.read.options(Map("inferSchema"->"false","delimiter"->"-","header"->"false")).csv(path)
   // spark.sparkContext.textFile(path)
  }

  /**
    * Added by Brahmdev Kumar
    * convert collection columns to rows
    * @param ds
    * @param selectColumns to be returns after selection
    * @param colNameToExplode containing Array
    * @return DataFrame    *
    */

  def getDatasetColumnArrayToRow(ds: DataFrame, selectColumns: Array[String], colNameToExplode : String): DataFrame={
    val colNames = selectColumns.map(name => col(name))
    ds.withColumn(colNameToExplode, explode(col(colNameToExplode)))
      .select(colNames: _*)
  }

}
