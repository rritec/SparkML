package com.optum.providerdashboard.common
import scala.reflect.runtime.universe._
import java.io.File
import java.sql.Date
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar

import com.optum.providerdashboard.common.Logger.log
import com.optum.providerdashboard.common.Exceptions.NoSuchPathException
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * Created by docker on 9/7/17.
  */
object Lib {

  def getCurrentTimestamp: java.sql.Timestamp =
    new java.sql.Timestamp(Calendar.getInstance().getTime().getTime())

  def getCurrentDate: java.sql.Date =
    new Date(Calendar.getInstance.getTime.getTime)

  def StringtoDate(in: String, format: String): java.sql.Date = {
    val format1 = new SimpleDateFormat(format).parse(in)
    val aDate = new java.sql.Date(format1.getTime())
    aDate
  }
  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {"%02x".format(_)}.foldLeft("") {_ + _}

  case class CinfoCC(var_name:String,var_type: String)

  def SaveParquet(input : DataFrame,Plist: List[String],outputDest : String):Boolean = {
    try {

      if (Plist.length == 0) {input.write
        .mode("overwrite")
        .format("parquet").save(outputDest)} else
      {input.write.partitionBy(Plist.mkString(","))
        .mode("overwrite")
        .format("parquet").save(outputDest)}



      true
    } catch {
      case e: Exception => e.printStackTrace()
        false
    }

  }
  def Save[T](input: Dataset[T], outputDest: String, destTable: String): Boolean = {
    try {
      println("SAVE function inside : " + Lib.getTimeFormatted())

      //input.toDF().write.mode("append").format("parquet").save(outputDest)
      //input.toDF().printSchema()
      //val finalsave=input.toDF().cache()
      input.toDF().write.mode("append").option("path", outputDest + "/" + destTable).parquet(outputDest + "/" + destTable)

      true
    } catch {
      case e: Exception => e.printStackTrace()
        false
    }
  }


  def classAccessors[T: TypeTag]: List[CinfoCC] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => new CinfoCC(m.name.toString.toUpperCase(),
      m.typeSignature.toString.substring(3).toUpperCase()
    ) }.toList

  def ScalaToHiveTypeTranslation(ScalaTypeList:List[CinfoCC]): List[CinfoCC]  ={
    ScalaTypeList.map(s => new CinfoCC(s.var_name,
      {s.var_type.replace("SCALA.","") match
      {
        case "LONG" => "BIGINT"
        case "DOUBLE" => "Double"
        case "INT" => "INT"
        case "STRING" => "STRING"
        case "JAVA.SQL.DATE" => "DATE"
        case _ => s.var_name + ": ERROR"
      }}    ) )  }


  def generateHiveViewCommand(      DatabaseName: String,
                                    TableName:String,
                                    ColumnsList: List[CinfoCC],
                                    RowFormatSERDE:String,
                                    PartitionColumnList: List[String],
                                    ParquetFileLocation: String,
                                    InputFormat:String,
                                    OutputFormat : String,
                                    Location:String
                             ): String ={

    val expr = "CREATE EXTERNAL TABLE "+ DatabaseName +"." + TableName + "(" +
      {ColumnsList.flatMap(s => Seq(s.var_name+" "+s.var_type)).mkString(",")} +
      ")  PARTITIONED BY (" +
      {ColumnsList.filter(s => PartitionColumnList.contains(s.var_name))
        .flatMap(s => Seq(s.var_name+" "+s.var_type)).mkString(",")} + ")" +
      "ROW FORMAT SERDE '" + RowFormatSERDE + "'" +
      " STORED AS " +
      "OUTPUTFORMAT '" + OutputFormat + "'" +
      "LOCATION '"+Location +"'"
    expr
  }



  def runSql(sparkSession: SparkSession, sqlQuery: String): DataFrame = {
    sparkSession.sql("set spark.sql.caseSensitive=false")
    sparkSession.sql(sqlQuery)
  }

  def toInt(s: String): Int = {
    try {
      if(s.isEmpty){
        0
      }
      else
        s.toInt
    } catch {
      case e: Exception => 0
    }
  }

  def toDouble(s: String): Double = {
    try {
      if(s.isEmpty){
        0.0
      }
      else
        s.toDouble
    } catch {
      case e: Exception => 0.0
    }
  }

  def toLong(s: String): Long = {
    try {
      if(s.isEmpty){
        0
      }
      else
        s.toLong
    } catch {
      case e: Exception => 0
    }
  }

  def toStr(s:String):String =if (s == null) new String() else s

  def toDate(s: String, format: String): java.sql.Date = {

    val formatter: DateFormat = new SimpleDateFormat(format)
    try {
      val  date_ : java.util.Date = formatter.parse(s)
      new java.sql.Date(date_.getTime())
    } catch {
      case e: Exception => {val  date_e : java.util.Date = formatter.parse("01/01/7777")
        new java.sql.Date(date_e.getTime())
      }
      //            println(s+" not parsing for ")
    }
  }



  def getTimeFormatted():String ={
    val today = Calendar.getInstance()
    val month = today.get(Calendar.MONTH)
    val year = today.get(Calendar.YEAR)
    val day = today.get(Calendar.DAY_OF_MONTH)
    val hour = today.get(Calendar.HOUR)
    val minute = today.get(Calendar.MINUTE)
    val second = today.get(Calendar.SECOND)
    s"$month$day$year$hour$minute$second"

  }

  def createDataFrame(sparkSession: SparkSession, path: String, fileFormat: String): DataFrame = {
    if (path == null || path.isEmpty()) {
      throw NoSuchPathException("Path is empty")
    }
    sparkSession.read.format(fileFormat).load(path)
  }


  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def deleteListOfFiles(dir: String): Unit = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.delete()
    }
  }

  def saveAsFile(df: DataFrame, format: String, path: String) {
    log.info("Saving the dataframe"+df.rdd.name+" to the path"+path)
    if (null == format)
      df.write.save(path)
    else
      df.write.format(format).save(path)
  }

  def saveAsFileforDataset[T](df: Dataset[T], format: String, path: String) {
    log.info("Saving the dataset"+df.rdd.name+" to the path"+path)
    if (null == format)
      df.write.save(path)
    else
      df.write.format(format).save(path)
  }


  def PrintCMDargs(args: Array[String])=args.foreach(println)

  def saveDataframeAsFile(srcDataframe: DataFrame, path: String): Unit = {
    srcDataframe.write.mode("overwrite").save(path)
  }




}