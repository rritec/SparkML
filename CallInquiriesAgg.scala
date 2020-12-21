package com.optum.providerdashboard.etl
import com.optum.providerdashboard.common.{ GlobalContext, DateApp, LoggerApp, SparkMongoLoad }

import org.apache.spark.sql.SparkSession

import java.util.{ Date, Calendar }
import java.text.SimpleDateFormat

object CallInquiriesAgg {

  def main(args: Array[String]) {
    try {
      val spark = GlobalContext.createSparkSession("Craeting Parquet file for Call Inquiries Metrics")
      LoggerApp.logger.info("Parquet File creation  for Call Inquiries Metrics Started at : " + DateApp.getDateTime())

      //Define Parquet input and output paths

      val callinquiries_file = spark.conf.get("spark.input.callinquiries")
      val provider_lookup_file = spark.conf.get("spark.input.providerLookup")
      val outputParquetPathByLOB = spark.conf.get("spark.output.outputParquetPathByLOB")
      val outputParquetPathByQType = spark.conf.get("spark.output.outputParquetPathByQType")

      println("Configurations Captured.")

      val callInquiriesDFLoad = spark.read.parquet(callinquiries_file)
      val providerLkpDF = spark.read.parquet(provider_lookup_file)

      println("Input files loaded.")

      callInquiriesDFLoad.createOrReplaceTempView("CallInquiries")
      providerLkpDF.createOrReplaceTempView("providerLkp")

      println("Views Created.")

      val startdate = spark.sql("select cast(date_sub(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd')),365) as string)").head().getString(0);
      val enddate = spark.sql("select cast(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd') as string)").head().getString(0);

      val ByLOBDF = spark.sql(s"""select "$startdate" as startdate , "$enddate" as enddate ,providersyskey, healthcareorganizationname as providerName,
					sum(CASE WHEN lob_id = 'E&I' THEN callVolume END) as EICALLVOLUME,
					sum(CASE WHEN lob_id = 'M&R' THEN callVolume END) as MRCALLVOLUME,
					sum(CASE WHEN lob_id = 'C&S' THEN callVolume END) as CSCALLVOLUME 
					from CallInquiries ci ,providerLkp ps 
					where ps.tin=ci.tin  
					and concat(substr(ci.calldate,1,4),'-',substr(ci.calldate,5,2),'-',substr(ci.calldate,7,2)) between "$startdate" and "$enddate" 
					group by startdate,enddate,providersyskey,providerName,lob_id""")

      ByLOBDF.createOrReplaceTempView("ByLOBDF");

      val ByLOBDFFinal = spark.sql(s"""select concat(startdate,'_',enddate) as ReportingPeriod,providersyskey,providerName,
					sum(EICALLVOLUME) as EIVolume,sum(MRCALLVOLUME) as MRVolume,sum(CSCALLVOLUME) as CSVolume 
					from ByLOBDF 
					group by startdate,enddate,providersyskey,providerName""");

      println("ByLOBDFFinal SQL Executed.")

      val ByQuestionTypeDF = spark.sql(s"""select  "$startdate" as startdate , "$enddate" as enddate  ,providersyskey, healthcareorganizationname as providerName,questionType,
					sum(CASE WHEN questionType = 'Benefits' THEN callVolume END) as BenefitsCALLVOLUME,
					sum(CASE WHEN questionType = 'Claims' THEN callVolume END) as ClaimsCALLVOLUME,
					sum(CASE WHEN questionType = 'Others' THEN callVolume END) as OthersCALLVOLUME,
					sum(CASE WHEN questionType = 'Prior Auth' THEN callVolume END) as PriorAuthCALLVOLUME,
					sum(CASE WHEN questionType = 'Benefits' THEN handleTime END) as BenefitsCALLTIME,
					sum(CASE WHEN questionType = 'Claims' THEN handleTime END) as ClaimsCALLTIME,
					sum(CASE WHEN questionType = 'Others' THEN handleTime END) as OthersCALLTIME,
					sum(CASE WHEN questionType = 'Prior Auth' THEN handleTime END) as PriorAuthCALLTIME
					from CallInquiries ci ,providerLkp ps 
					where ps.tin=ci.tin  
					and concat(substr(ci.calldate,1,4),'-',substr(ci.calldate,5,2),'-',substr(ci.calldate,7,2)) between "$startdate" and "$enddate" 
					group by startdate,enddate,providersyskey,providerName,questionType""")

      ByQuestionTypeDF.createOrReplaceTempView("ByQuestionTypeDF");

      val ByQuestionTypeDFFinal = spark.sql(s"""select concat(startdate,'_',enddate) as ReportingPeriod,providersyskey,providerName,
					sum(BenefitsCALLVOLUME) as BenefitsCALLVOLUME,
					sum(ClaimsCALLVOLUME) as ClaimsCALLVOLUME,
					sum(OthersCALLVOLUME) as OthersCALLVOLUME,
					sum(PriorAuthCALLVOLUME) as PriorAuthCALLVOLUME,
					sum(BenefitsCALLTIME) as BenefitsCALLTIME,
					sum(ClaimsCALLTIME) as ClaimsCALLTIME,
					sum(PriorAuthCALLTIME) as PriorAuthCALLTIME,
					sum(OthersCALLTIME) as OthersCALLTIME 
					from ByQuestionTypeDF 
					group by startdate,enddate,providersyskey,providerName""");

      println("ByQuestionTypeDFFinal Executed.")

      ByLOBDFFinal.write.mode("overwrite").parquet(outputParquetPathByLOB)
      ByQuestionTypeDFFinal.write.mode("overwrite").parquet(outputParquetPathByQType)
      println("LOB and QType parquet files creation Job Completed")
    } catch {
      case e: Exception => LoggerApp.logger.info(s"Exception while Creating Parquet Files for Executive Summary Dashboard" :+ e.printStackTrace())
    }
  }

}
