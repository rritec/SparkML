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
object ProvTINClmAgg {
def main(args: Array[String]): Unit = {
try
  {
val spark = GlobalContext.createSparkSession("JSON Document Creation for Provider-TIN Level Claims Aggregation Collection")
LoggerApp.logger.info("JSON Documents creation  for Provider-TIN Level Claims Aggregation Started at : " + DateApp.getDateTime())
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
val mongocollection  = spark.sparkContext.getConf.get("spark.mongo.ProvTINClmAgg")
println("mongocollection :" + mongocollection)
val passwordpath  = spark.sparkContext.getConf.get("spark.input.passwordpath")
val passphrase  = spark.sparkContext.getConf.get("spark.input.passphrase")
val outputProvTINClmAgg = spark.sparkContext.getConf.get("spark.output.ProvTINClmAgg")
println("outputProvTINClmAgg :" + outputProvTINClmAgg)
println("ProvTINClmAgg - Table: " + System.currentTimeMillis())
val command: String = "gpg --batch  --passphrase "+passphrase+" --decrypt "+passwordpath;  
val mongopassword = DecryptionPassword.getPassword(command)
val source = spark.sql(s"""select pcs.*,pt.providersyskey,pt.healthcareorganizationname as healthcareorganization,pt.formattedtin as formattedtin from $pvddbname.$pedprvdsystbl pt join $bicdbname.$bicclmtable pcs on pt.tin=pcs.providertin""")
source.createOrReplaceTempView("source")

val df1 = spark.sql(s"""select  providersyskey,year(IncurredServiceDate) as ReportingPeriod,HealthcareOrganization as ProviderSystem,BusinessSegmentIdentifier,formattedtin as TIN,ClaimDenialCategoryLevel1ShortName,sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as DenialCount,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as DenialAmount from source where PaidOrDeniedCode = 'D' group by year(IncurredServiceDate),HealthcareOrganization,formattedtin,BusinessSegmentIdentifier,ClaimDenialCategoryLevel1ShortName,providersyskey""")
df1.createOrReplaceTempView("df1")
val dfGrp1 = spark.sql("select * from df1 ")
val dfGrp2 = spark.sql(s"""select providersyskey,year(IncurredServiceDate) as ReportingPeriod,HealthcareOrganization as ProviderSystem,BusinessSegmentIdentifier,formattedtin as TIN,sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsDenied,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as AmountDenied,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END) as ClaimsPaid,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountPaid,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END)+sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsSubmitted,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END)+Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountSubmitted,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END)+sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsCompleted,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END)+Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountCompleted,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountUHCPaid,Sum(case when FinalPassIndicator = 'Y' THEN BilledAmount ELSE 0 END) as AmountBilled,Sum( ActualAllowedAmount) as AmountActualAllowed,Sum( ActualAllowedAmount)+sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as AmountExpectedAllowed from source group by year(IncurredServiceDate),HealthcareOrganization,formattedtin,BusinessSegmentIdentifier,providersyskey""")

val df3 = spark.sql(s"""select  providersyskey,year(IncurredServiceDate) as ReportingPeriod,HealthcareOrganization as ProviderSystem,formattedtin as TIN,ClaimDenialCategoryLevel1ShortName,sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as DenialCount,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as DenialAmount from source where PaidOrDeniedCode = 'D' group by year(IncurredServiceDate),HealthcareOrganization,formattedtin,ClaimDenialCategoryLevel1ShortName,providersyskey""")
df3.createOrReplaceTempView("df3")
val dfGrp3 = spark.sql("select * from df3 ")
val dfGrp4 = spark.sql(s"""select providersyskey,year(IncurredServiceDate) as ReportingPeriod,HealthcareOrganization as ProviderSystem,formattedtin as TIN,sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsDenied,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as AmountDenied,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END) as ClaimsPaid,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountPaid,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END)+sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsSubmitted,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END)+Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountSubmitted,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN masteridclaimcount ELSE 0 END)+sum(case when PaidOrDeniedCode = 'D' THEN masteridclaimcount ELSE 0 END) as ClaimsCompleted,sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END)+Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountCompleted,Sum(case when PaidOrDeniedCode = 'P' and FinalPassIndicator = 'Y' THEN UHCPaidAmount ELSE 0 END) as AmountUHCPaid,Sum(case when FinalPassIndicator = 'Y' THEN BilledAmount ELSE 0 END) as AmountBilled,Sum( ActualAllowedAmount) as AmountActualAllowed,Sum( ActualAllowedAmount)+sum(case when PaidOrDeniedCode = 'D' THEN EstimatedAllowedSelectDeniedAmount ELSE 0 END) as AmountExpectedAllowed from source group by year(IncurredServiceDate),HealthcareOrganization,formattedtin,providersyskey""")

val EIFilterGrp1 = dfGrp1.filter(col("businesssegmentidentifier") === "E&I")
val EIGrp1DnlCtg = EIFilterGrp1.withColumn("DenialCategory",struct("claimdenialcategorylevel1shortname","DenialCount","DenialAmount"))
val EIGrp1Col = EIGrp1DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("TIN"),col("ProviderSystem")).agg(collect_list(col("DenialCategory")).as("DenialCategory"))

val EIFilterGrp2 = dfGrp2.filter(col("businesssegmentidentifier") === "E&I")
val EIGrp2DnlCtg = EIFilterGrp2.withColumn("Others",struct(col("ClaimsPaid"),col("ClaimsDenied"),col("AmountDenied"),col("AmountPaid"),col("ClaimsSubmitted"),col("AmountSubmitted"),col("ClaimsCompleted"),col("AmountCompleted"),col("AmountUHCPaid"),col("AmountBilled"),col("AmountActualAllowed"),col("AmountExpectedAllowed")))
val EIGrp2Col = EIGrp2DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("TIN"),col("ProviderSystem")).agg(collect_list(col("Others")).as("ClaimsLobSummary"))

val EIJoin =  EIGrp1Col.join(EIGrp2Col,Seq("businesssegmentidentifier","ReportingPeriod","TIN","ProviderSystem","providersyskey"),"fullouter")
val EIJoinStct = EIJoin.withColumn("EI",struct(col("ClaimsLobSummary"),col("DenialCategory")))
val EIFinal = EIJoinStct.select(col("ProviderSystem"),col("providersyskey").as("ProviderKey"),col("TIN"),col("ReportingPeriod"),col("EI"))

val MRFilterGrp1 = dfGrp1.filter(col("businesssegmentidentifier") === "M&R")
val MRGrp1DnlCtg = MRFilterGrp1.withColumn("DenialCategory",struct("claimdenialcategorylevel1shortname","DenialCount","DenialAmount"))
val MRGrp1Col = MRGrp1DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("TIN"),col("ProviderSystem")).agg(collect_list(col("DenialCategory")).as("DenialCategory"))

val MRFilterGrp2 = dfGrp2.filter(col("businesssegmentidentifier") === "M&R")
val MRGrp2DnlCtg = MRFilterGrp2.withColumn("Others",struct(col("ClaimsPaid"),col("ClaimsDenied"),col("AmountDenied"),col("AmountPaid"),col("ClaimsSubmitted"),col("AmountSubmitted"),col("ClaimsCompleted"),col("AmountCompleted"),col("AmountUHCPaid"),col("AmountBilled"),col("AmountActualAllowed"),col("AmountExpectedAllowed")))
val MRGrp2Col = MRGrp2DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("TIN"),col("ProviderSystem")).agg(collect_list(col("Others")).as("ClaimsLobSummary"))

val MRJoin =  MRGrp1Col.join(MRGrp2Col,Seq("businesssegmentidentifier","ReportingPeriod","TIN","ProviderSystem","providersyskey"),"fullouter")
val MRJoinStct = MRJoin.withColumn("MR",struct(col("ClaimsLobSummary"),col("DenialCategory")))
val MRFinal = MRJoinStct.select(col("ProviderSystem"),col("providersyskey").as("ProviderKey"),col("TIN"),col("ReportingPeriod"),col("MR"))

val CSFilterGrp1 = dfGrp1.filter(col("businesssegmentidentifier") === "C&S")
val CSGrp1DnlCtg = CSFilterGrp1.withColumn("DenialCategory",struct("claimdenialcategorylevel1shortname","DenialCount","DenialAmount"))
val CSGrp1Col = CSGrp1DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("TIN"),col("ProviderSystem")).agg(collect_list(col("DenialCategory")).as("DenialCategory"))

val CSFilterGrp2 = dfGrp2.filter(col("businesssegmentidentifier") === "C&S")
val CSGrp2DnlCtg = CSFilterGrp2.withColumn("Others",struct(col("ClaimsPaid"),col("ClaimsDenied"),col("AmountDenied"),col("AmountPaid"),col("ClaimsSubmitted"),col("AmountSubmitted"),col("ClaimsCompleted"),col("AmountCompleted"),col("AmountUHCPaid"),col("AmountBilled"),col("AmountActualAllowed"),col("AmountExpectedAllowed")))
val CSGrp2Col = CSGrp2DnlCtg.groupBy(col("businesssegmentidentifier"),col("providersyskey"),col("ReportingPeriod"),col("TIN"),col("ProviderSystem")).agg(collect_list(col("Others")).as("ClaimsLobSummary"))

val CSJoin =  CSGrp1Col.join(CSGrp2Col,Seq("businesssegmentidentifier","ReportingPeriod","TIN","ProviderSystem","providersyskey"),"fullouter")
val CSJoinStct = CSJoin.withColumn("CS",struct(col("ClaimsLobSummary"),col("DenialCategory")))
val CSFinal = CSJoinStct.select(col("ProviderSystem"),col("providersyskey").as("ProviderKey"),col("TIN"),col("ReportingPeriod"),col("CS"))

val ALLGrp1DnlCtg = dfGrp3.withColumn("DenialCategory",struct("claimdenialcategorylevel1shortname","DenialCount","DenialAmount"))
val ALLGrp1Col = ALLGrp1DnlCtg.groupBy(col("ReportingPeriod"),col("providersyskey"),col("TIN"),col("ProviderSystem")).agg(collect_list(col("DenialCategory")).as("DenialCategory"))

val ALLGrp2DnlCtg = dfGrp4.withColumn("Others",struct(col("ClaimsPaid"),col("ClaimsDenied"),col("AmountDenied"),col("AmountPaid"),col("ClaimsSubmitted"),col("AmountSubmitted"),col("ClaimsCompleted"),col("AmountCompleted"),col("AmountUHCPaid"),col("AmountBilled"),col("AmountActualAllowed"),col("AmountExpectedAllowed")))
val ALLGrp2Col = ALLGrp2DnlCtg.groupBy(col("ReportingPeriod"),col("providersyskey"),col("TIN"),col("ProviderSystem")).agg(collect_list(col("Others")).as("ClaimsLobSummary"))

val ALLJoin =  ALLGrp1Col.join(ALLGrp2Col,Seq("ReportingPeriod","TIN","ProviderSystem","providersyskey"),"fullouter")
val ALLJoinStct = ALLJoin.withColumn("ALL",struct(col("ClaimsLobSummary"),col("DenialCategory")))
val ALLFinal = ALLJoinStct.select(col("ProviderSystem"),col("providersyskey").as("ProviderKey"),col("TIN"),col("ReportingPeriod"),col("ALL"))

val FinalJoin = EIFinal.join(CSFinal,Seq("ProviderSystem","ProviderKey","TIN","ReportingPeriod"),"fullouter").join(MRFinal,Seq("ProviderSystem","ProviderKey","TIN","ReportingPeriod"),"fullouter").join(ALLFinal,Seq("ProviderSystem","ProviderKey","TIN","ReportingPeriod"),"fullouter")
val JSONDoc = FinalJoin.select(col("ProviderSystem"),col("ProviderKey"),col("TIN"),col("ReportingPeriod"),col("EI"),col("CS"),col("MR"),col("ALL")).dropDuplicates()

JSONDoc.repartition(1).write.mode("overwrite").format("json").save("maprfs://".concat(outputProvTINClmAgg))
LoggerApp.logger.info("JSON Documents creation  for Provider-TIN Level Claims Aggregation Ended at : " + DateApp.getDateTime())
LoggerApp.logger.info("Loading Provider-TIN Level Claims Aggregation data into Mongo Collection:ProvTINClmAgg Started at : " + DateApp.getDateTime())
val loc = "maprfs://".concat(outputProvTINClmAgg)
SparkMongoLoad.MongoLoad(loc, mongodbname, mongocollection, mongohost, mongoport, mongouname, mongopassword)
LoggerApp.logger.info("Loading Provider-TIN Level Claims Aggregation data into Mongo Collection:ProvTINClmAgg Ended at : " + DateApp.getDateTime())
}
catch {
      case e: Exception => LoggerApp.logger.info(s"Exception while JSON Documents creation for Provider-TIN Level Claims Aggregation Collection" :+ e.printStackTrace())
    }

}
}