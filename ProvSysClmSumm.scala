package com.optum.providerdashboard.etl
import com.optum.providerdashboard.common.{GlobalContext,DateApp,LoggerApp,SparkMongoLoad,DecryptionPassword}

import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.MongoSpark
object ProvSysClmSumm {
def main(args: Array[String]): Unit = {
try
  {
val spark = GlobalContext.createSparkSession("JSON Document Creation for ProviderSystem Level Claims Summary Collection")
LoggerApp.logger.info("JSON Documents creation  for ProviderSystem Level Claims Summary Started at : " + DateApp.getDateTime())
val bicdbname  = spark.sparkContext.getConf.get("spark.bic.dbname")
println("bicdbname :" + bicdbname)
val pvddbname  = spark.sparkContext.getConf.get("spark.provdbname")
println("pvddbname :" + pvddbname)
val bicclmtable  = spark.sparkContext.getConf.get("spark.bic.clmpassmrytbl_name")
println("bicclmtable :" + bicclmtable)
val pedbatchid  = spark.sparkContext.getConf.get("spark.pedbatchid")
println("pedbatchid :" + pedbatchid)
val pedprvdsystbl  = spark.sparkContext.getConf.get("spark.pedprvdsystbl")
println("pedprvdsystbl :" + pedprvdsystbl)
val mongodbname  = spark.sparkContext.getConf.get("spark.mongo.dbname")
println("mongodbname :" + mongodbname)
val mongohost  = spark.sparkContext.getConf.get("spark.mongo.host")
println("mongohost :" + mongohost)
val mongoport  = spark.sparkContext.getConf.get("spark.mongo.port").toInt
println("mongoport :" + mongoport)
val mongouname  = spark.sparkContext.getConf.get("spark.mongo.usernaame")
//val mongopassword  = spark.sparkContext.getConf.get("spark.mongo.password")
val mongocollection  = spark.sparkContext.getConf.get("spark.mongo.ProvSysClmSumm")
println("mongocollection :" + mongocollection)
val outputProvSysClmSumm = spark.sparkContext.getConf.get("spark.output.ProvSysClmSumm")
println("outputProvSysClmSumm :" + outputProvSysClmSumm)
println("ProvSysClmSumm - Table: " + System.currentTimeMillis())
val passwordpath  = spark.sparkContext.getConf.get("spark.input.passwordpath")
val passphrase  = spark.sparkContext.getConf.get("spark.input.passphrase")
val command: String = "gpg --batch  --passphrase "+passphrase+" --decrypt "+passwordpath;  
val mongopassword = DecryptionPassword.getPassword(command)
val source = spark.sql(s"""select pcs.*,pt.providersyskey,pt.healthcareorganizationname as healthcareorganization from $pvddbname.$pedprvdsystbl pt join $bicdbname.$bicclmtable pcs on pt.tin=pcs.providertin""")
source.createOrReplaceTempView("source")
val df1 = spark.sql(s"""select  providersyskey,case when month(IncurredServiceDate) < 10 then concat(0,month(IncurredServiceDate),year(IncurredServiceDate)) when month(IncurredServiceDate) >= 10 then concat(month(IncurredServiceDate),year(IncurredServiceDate)) else 0 end as ReportingPeriod,HealthcareOrganization as ProviderSystem,BusinessSegmentIdentifier,ClaimDenialCategoryLevel1ShortName,sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as DenialCount,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as DenialAmount from source where PaidOrDeniedCode = 'D' group by case when month(IncurredServiceDate) < 10 then concat(0,month(IncurredServiceDate),year(IncurredServiceDate)) when month(IncurredServiceDate) >= 10 then concat(month(IncurredServiceDate),year(IncurredServiceDate)) else 0 end,HealthcareOrganization,BusinessSegmentIdentifier,ClaimDenialCategoryLevel1ShortName,providersyskey""")
df1.createOrReplaceTempView("df1")
val dfGrp1 = spark.sql("select * from df1 ")
val dfGrp2 = spark.sql(s"""select providersyskey,case when month(IncurredServiceDate) < 10 then concat(0,month(IncurredServiceDate),year(IncurredServiceDate)) when month(IncurredServiceDate) >= 10 then concat(month(IncurredServiceDate),year(IncurredServiceDate)) else 0 end as ReportingPeriod,HealthcareOrganization as ProviderSystem,BusinessSegmentIdentifier,sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsDenied,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as AmountDenied,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END) as ClaimsPaid,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountPaid,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END)+sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsSubmitted,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END)+Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountSubmitted,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END)+sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsCompleted,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END)+Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountCompleted,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountUHCPaid,Sum(case when FinalPassIndicator = 'Y' THEN BilledAmount ELSE 0 END) as AmountBilled,Sum(ActualAllowedAmount) as AmountActualAllowed,Sum(ActualAllowedAmount)+sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as AmountExpectedAllowed from source group by case when month(IncurredServiceDate) < 10 then concat(0,month(IncurredServiceDate),year(IncurredServiceDate)) when month(IncurredServiceDate) >= 10 then concat(month(IncurredServiceDate),year(IncurredServiceDate)) else 0 end,HealthcareOrganization,BusinessSegmentIdentifier,providersyskey""")

val df3 = spark.sql(s"""select  providersyskey,case when month(IncurredServiceDate) < 10 then concat(0,month(IncurredServiceDate),year(IncurredServiceDate)) when month(IncurredServiceDate) >= 10 then concat(month(IncurredServiceDate),year(IncurredServiceDate)) else 0 end as ReportingPeriod,HealthcareOrganization as ProviderSystem,ClaimDenialCategoryLevel1ShortName,sum(case when PaidOrDeniedCode = 'D' then masteridclaimcount ELSE 0 END) as DenialCount,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as DenialAmount from source where PaidOrDeniedCode = 'D' group by case when month(IncurredServiceDate) < 10 then concat(0,month(IncurredServiceDate),year(IncurredServiceDate)) when month(IncurredServiceDate) >= 10 then concat(month(IncurredServiceDate),year(IncurredServiceDate)) else 0 end,HealthcareOrganization,ClaimDenialCategoryLevel1ShortName,providersyskey""")
df3.createOrReplaceTempView("df3")
val dfGrp3 = spark.sql("select * from df3 ")
val dfGrp4 = spark.sql(s"""select providersyskey,case when month(IncurredServiceDate) < 10 then concat(0,month(IncurredServiceDate),year(IncurredServiceDate)) when month(IncurredServiceDate) >= 10 then concat(month(IncurredServiceDate),year(IncurredServiceDate)) else 0 end as ReportingPeriod,HealthcareOrganization as ProviderSystem,sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsDenied,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as AmountDenied,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END) as ClaimsPaid,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountPaid,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END)+sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsSubmitted,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END)+Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountSubmitted,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END)+sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsCompleted,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END)+Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountCompleted,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountUHCPaid,Sum(case when FinalPassIndicator = 'Y' THEN BilledAmount ELSE 0 END) as AmountBilled,Sum(ActualAllowedAmount) as AmountActualAllowed,Sum(ActualAllowedAmount)+sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as AmountExpectedAllowed from source group by case when month(IncurredServiceDate) < 10 then concat(0,month(IncurredServiceDate),year(IncurredServiceDate)) when month(IncurredServiceDate) >= 10 then concat(month(IncurredServiceDate),year(IncurredServiceDate)) else 0 end,HealthcareOrganization,providersyskey""")

val EIFilterGrp1 = dfGrp1.filter(col("businesssegmentidentifier") === "E&I")
val EIGrp1DnlCtg = EIFilterGrp1.withColumn("DenialCategory",struct("claimdenialcategorylevel1shortname","DenialCount","DenialAmount"))
val EIGrp1Col = EIGrp1DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("ProviderSystem")).agg(collect_list(col("DenialCategory")).as("DenialCategory"))

val EIFilterGrp2 = dfGrp2.filter(col("businesssegmentidentifier") === "E&I")
val EIGrp2DnlCtg = EIFilterGrp2.withColumn("Others",struct(col("ClaimsPaid"),col("ClaimsDenied"),col("AmountDenied"),col("AmountPaid"),col("ClaimsSubmitted"),col("AmountSubmitted"),col("ClaimsCompleted"),col("AmountCompleted"),col("AmountUHCPaid"),col("AmountBilled"),col("AmountActualAllowed"),col("AmountExpectedAllowed")))
val EIGrp2Col = EIGrp2DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("ProviderSystem")).agg(collect_list(col("Others")).as("ClaimsLobSummary"))

val EIJoin =  EIGrp1Col.join(EIGrp2Col,Seq("businesssegmentidentifier","ReportingPeriod","ProviderSystem","providersyskey"),"fullouter")
val EIJoinStct = EIJoin.withColumn("EI",struct(col("ClaimsLobSummary"),col("DenialCategory")))
val EIFinal = EIJoinStct.select(col("ProviderSystem"),col("providersyskey").as("ProviderKey"),col("ReportingPeriod"),col("EI"))

val MRFilterGrp1 = dfGrp1.filter(col("businesssegmentidentifier") === "M&R")
val MRGrp1DnlCtg = MRFilterGrp1.withColumn("DenialCategory",struct("claimdenialcategorylevel1shortname","DenialCount","DenialAmount"))
val MRGrp1Col = MRGrp1DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("ProviderSystem")).agg(collect_list(col("DenialCategory")).as("DenialCategory"))

val MRFilterGrp2 = dfGrp2.filter(col("businesssegmentidentifier") === "M&R")
val MRGrp2DnlCtg = MRFilterGrp2.withColumn("Others",struct(col("ClaimsPaid"),col("ClaimsDenied"),col("AmountDenied"),col("AmountPaid"),col("ClaimsSubmitted"),col("AmountSubmitted"),col("ClaimsCompleted"),col("AmountCompleted"),col("AmountUHCPaid"),col("AmountBilled"),col("AmountActualAllowed"),col("AmountExpectedAllowed")))
val MRGrp2Col = MRGrp2DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("ProviderSystem")).agg(collect_list(col("Others")).as("ClaimsLobSummary"))

val MRJoin =  MRGrp1Col.join(MRGrp2Col,Seq("businesssegmentidentifier","ReportingPeriod","ProviderSystem","providersyskey"),"fullouter")
val MRJoinStct = MRJoin.withColumn("MR",struct(col("ClaimsLobSummary"),col("DenialCategory")))
val MRFinal = MRJoinStct.select(col("ProviderSystem"),col("providersyskey").as("ProviderKey"),col("ReportingPeriod"),col("MR"))

val CSFilterGrp1 = dfGrp1.filter(col("businesssegmentidentifier") === "C&S")
val CSGrp1DnlCtg = CSFilterGrp1.withColumn("DenialCategory",struct("claimdenialcategorylevel1shortname","DenialCount","DenialAmount"))
val CSGrp1Col = CSGrp1DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("ProviderSystem")).agg(collect_list(col("DenialCategory")).as("DenialCategory"))

val CSFilterGrp2 = dfGrp2.filter(col("businesssegmentidentifier") === "C&S")
val CSGrp2DnlCtg = CSFilterGrp2.withColumn("Others",struct(col("ClaimsPaid"),col("ClaimsDenied"),col("AmountDenied"),col("AmountPaid"),col("ClaimsSubmitted"),col("AmountSubmitted"),col("ClaimsCompleted"),col("AmountCompleted"),col("AmountUHCPaid"),col("AmountBilled"),col("AmountActualAllowed"),col("AmountExpectedAllowed")))
val CSGrp2Col = CSGrp2DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("ProviderSystem")).agg(collect_list(col("Others")).as("ClaimsLobSummary"))

val CSJoin =  CSGrp1Col.join(CSGrp2Col,Seq("businesssegmentidentifier","ReportingPeriod","ProviderSystem","providersyskey"),"fullouter")
val CSJoinStct = CSJoin.withColumn("CS",struct(col("ClaimsLobSummary"),col("DenialCategory")))
val CSFinal = CSJoinStct.select(col("ProviderSystem"),col("providersyskey").as("ProviderKey"),col("ReportingPeriod"),col("CS"))

val ALLGrp1DnlCtg = dfGrp3.withColumn("DenialCategory",struct("claimdenialcategorylevel1shortname","DenialCount","DenialAmount"))
val ALLGrp1Col = ALLGrp1DnlCtg.groupBy(col("ReportingPeriod"),col("providersyskey"),col("ProviderSystem")).agg(collect_list(col("DenialCategory")).as("DenialCategory"))

val ALLGrp2DnlCtg = dfGrp4.withColumn("Others",struct(col("ClaimsPaid"),col("ClaimsDenied"),col("AmountDenied"),col("AmountPaid"),col("ClaimsSubmitted"),col("AmountSubmitted"),col("ClaimsCompleted"),col("AmountCompleted"),col("AmountUHCPaid"),col("AmountBilled"),col("AmountActualAllowed"),col("AmountExpectedAllowed")))
val ALLGrp2Col = ALLGrp2DnlCtg.groupBy(col("ReportingPeriod"),col("providersyskey"),col("ProviderSystem")).agg(collect_list(col("Others")).as("ClaimsLobSummary"))

val ALLJoin =  ALLGrp1Col.join(ALLGrp2Col,Seq("ReportingPeriod","ProviderSystem","providersyskey"),"fullouter")
val ALLJoinStct = ALLJoin.withColumn("ALL",struct(col("ClaimsLobSummary"),col("DenialCategory")))
val ALLFinal = ALLJoinStct.select(col("ProviderSystem"),col("providersyskey").as("ProviderKey"),col("ReportingPeriod"),col("ALL"))

val FinalJoin = EIFinal.join(CSFinal,Seq("ProviderSystem","ProviderKey","ReportingPeriod"),"fullouter").join(MRFinal,Seq("ProviderSystem","ProviderKey","ReportingPeriod"),"fullouter").join(ALLFinal,Seq("ProviderSystem","ProviderKey","ReportingPeriod"),"fullouter")
val RP = FinalJoin.withColumn("Reportingperiod", concat(lit("01-"), col("Reportingperiod")))
val RPFinal = RP.withColumn("Reportingperiod", date_format(unix_timestamp(col("Reportingperiod"), "dd-MMyyyy").cast("timestamp"),"yyyy-MM-dd")).withColumn("Reportingperiod",col("Reportingperiod").cast("String"))

val JSONDoc = RPFinal.select(col("ProviderSystem"),col("ProviderKey"),col("ReportingPeriod"),col("EI"),col("CS"),col("MR"),col("ALL")).dropDuplicates()
JSONDoc.repartition(1).write.mode("overwrite").format("json").save("maprfs://".concat(outputProvSysClmSumm))
LoggerApp.logger.info("JSON Documents creation  for ProviderSystem Level Claims Summary Ended at : " + DateApp.getDateTime())
LoggerApp.logger.info("Loading ProviderSystem Level Claims Summary data into Mongo Collection:ProvSysClmSumm Started at : " + DateApp.getDateTime())
val loc = "maprfs://".concat(outputProvSysClmSumm)
SparkMongoLoad.MongoLoad(loc, mongodbname, mongocollection, mongohost, mongoport, mongouname, mongopassword)
LoggerApp.logger.info("Loading ProviderSystem Level Claims Summary data into Mongo Collection:ProvSysClmSumm Ended at : " + DateApp.getDateTime())
}
catch {
      case e: Exception => LoggerApp.logger.info(s"Exception while JSON Documents creation for ProviderSystem Level Claims Summary Collection" :+ e.printStackTrace())
    }
}
}