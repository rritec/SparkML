package com.optum.providerdashboard.etl
import com.optum.providerdashboard.common.{GlobalContext,DateApp,LoggerApp,SparkMongoLoad}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions.regexp_replace
object Selfservice {
def main(args: Array[String]): Unit = {
  try
  {

val spark = GlobalContext.createSparkSession("Craeting Parquet file for Self Service Metrics")
LoggerApp.logger.info("Parquet File creation  for Sef Serice Metrics Started at : " + DateApp.getDateTime())

 val iaddbname  = spark.sparkContext.getConf.get("spark.iad.dbname")
println("iaddbname :" + iaddbname)

val pvddbname  = spark.sparkContext.getConf.get("spark.provdbname")
println("pvddbname :" + pvddbname)

val ssmtable  = spark.sparkContext.getConf.get("spark.iad.selfservicetblname")
println("ssmtable :" + ssmtable)

val pedbatchid  = spark.sparkContext.getConf.get("spark.pedbatchid")
println("pedbatchid :" + pedbatchid)

val pedprvdsystbl  = spark.sparkContext.getConf.get("spark.pedprvdsystbl")
println("pedprvdsystbl :" + pedprvdsystbl)

val outputselfservice = spark.sparkContext.getConf.get("spark.output.Selfservice")
println("outputselfservice :" + outputselfservice)

val de = spark.sql(s"""select providersyskey, healthcareorganizationname, year*12+month as monthyear ,
sum( case when table='clm_proc' and data_coverage='3-month' and txn='ORIGINAL' and channel='Paper'  then avg_proc_time else 0 END)/sum( case when table='clm_proc' and data_coverage='3-month' and txn='ORIGINAL' and channel='Paper'  then 1 else 0 END) as avgpclmprctm,
sum( case when table='clm_proc' and data_coverage='3-month' and txn='ORIGINAL' and channel='Paper'  then clm_count else 0 END) as avgpclmcnt,
sum( case when table='clm_proc' and data_coverage='3-month' and txn='ORIGINAL' and channel='Electronic'  then avg_proc_time else 0 END)/sum( case when table='clm_proc' and data_coverage='3-month' and txn='ORIGINAL' and channel='Electronic'  then 1 else 0 END) as avgeclmprctm,
sum( case when table='clm_proc' and data_coverage='3-month' and txn='ORIGINAL' and channel='Electronic'  then clm_count else 0 END) as avgeclmcnt,
sum( case when table='clm_proc' and data_coverage='3-month' and txn='ADJUSTMENT' and channel='Paper'  then avg_proc_time else 0 END)/sum( case when table='clm_proc' and data_coverage='3-month' and txn='ADJUSTMENT' and channel='Paper'  then 1 else 0 END) as avgprecprctm,
sum( case when table='clm_proc' and data_coverage='3-month' and txn='ADJUSTMENT' and channel='Paper'  then clm_count else 0 END) as avgpreccnt,
sum( case when table='clm_proc' and data_coverage='3-month' and txn='ADJUSTMENT' and channel='Electronic'  then avg_proc_time else 0 END)/sum( case when table='clm_proc' and data_coverage='3-month' and txn='ADJUSTMENT' and channel='Electronic' then 1 else 0 END) as avgerecprctm,
sum( case when table='clm_proc' and data_coverage='3-month' and txn='ADJUSTMENT' and channel='Electronic'  then clm_count else 0 END) as avgereccnt,
sum( case when txn='ATH' and channel='calls' and year*12+month in((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-2,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-3,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-4) then txn_cnt else 0 END) as authcallcnt,
sum( case when txn='ELI' and channel='calls' and year*12+month in((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-2,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-3,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-4) then txn_cnt else 0 END) as eligcallcnt,
sum( case when txn='BEN' and channel='calls' and year*12+month in((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-2,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-3,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-4) then txn_cnt else 0 END) as bencallcnt,
sum( case when txn='CLM' and channel='calls' and year*12+month in((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-2,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-3,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-4) then txn_cnt else 0 END) as clmcallcnt,
sum( case when channel='calls' and txn in ('APL','ATH','BEN','CLM','ELI','MIS','Others') and year*12+month in((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-2,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-3,(year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-4) then txn_cnt else 0 END) as callcnt,
sum( case when channel='PnP_elec' then txn_cnt else 0 END) as PnPelec,
sum( case when channel='PnP_paper' then txn_cnt else 0 END) as PnPpaper,
sum( case when channel='calls' and txn in ('BEN','CLM','ELI') then txn_cnt else 0 END) as cnccall,
sum( case when channel='Payer' and substr(txn,-5,5)='Paper' then txn_cnt else 0 END) as cncpaper,
sum( case when channel in ('link_cm_searches','link_ebc_searches','EDI276','EDI270', 'elink_searches','clink_searches') or substr(txn,-3,3)='EDI' then txn_cnt else 0 END) as SelfServeCnC
from $iaddbname.$ssmtable ssd, $pvddbname.$pedprvdsystbl ps where ps.tin=ssd.tax_id group by providersyskey, healthcareorganizationname, year*12+month""").cache()

val dewithcl=de.withColumn("clmprctmdiff",abs(col("avgpclmprctm")-col("avgeclmprctm"))).withColumn("reconprctmdiff",abs(col("avgprecprctm")-col("avgerecprctm")))
dewithcl.createOrReplaceTempView("dewithcl")  
val dec2 = spark.sql("select *,(6.50-0.94)*callcnt/(60*90) as calltm,(7.5-1.89)*authcallcnt+(4.02-0.42)*eligcallcnt+(4.02-0.42)*bencallcnt+(5.4-1.81)*clmcallcnt as callcost,(5-((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-monthyear))*PnPelec/6 as PnPelecwt,(5-((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-monthyear))*PnPpaper/6 as PnPpaperwt,(5-((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-monthyear))*cnccall/6 as SizeOfOppcncwt,(5-((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-monthyear))*cncpaper/6 as SizeOfOpppnpwt,(5-((year(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))*12)+MONTH(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp(), 'yyyy-MM-dd'))))-monthyear))*SelfServeCnC/6 as SelfServeCnCwt from dewithcl") 
val dec3 = dec2.withColumn("pnpadptrt",(col("PnPelecwt")/(col("PnPelecwt")+col("PnPpaperwt")))).withColumn("lnkadptrt",(col("SelfServeCnCwt")/(col("SelfServeCnCwt")+col("SizeOfOppcncwt")+col("SizeOfOpppnpwt"))))   
dec3.createOrReplaceTempView("dec3")
val decgrp = spark.sql("select providersyskey as ProviderSysKey,healthcareorganizationname as ProviderSystem,sum(avgpclmprctm) as avgpclmprctm,sum(avgpclmcnt) as avgpclmcnt,sum(avgeclmprctm) as avgeclmprctm,sum(avgeclmcnt) as avgeclmcnt,sum(clmprctmdiff) as clmprctmdiff,sum(avgprecprctm) as avgprecprctm,sum(avgpreccnt) as avgpreccnt,sum(avgerecprctm) as avgerecprctm,sum(avgereccnt) as avgereccnt,sum(reconprctmdiff) as reconprctmdiff,sum(callcnt) as callcnt,sum(calltm) as calltm,sum(authcallcnt) as authcallcnt,sum(eligcallcnt) as eligcallcnt,sum(bencallcnt) as bencallcnt,sum(clmcallcnt) as clmcallcnt,sum(callcost) as callcost,sum(PnPelecwt) as PnPelecwt,sum(PnPpaperwt) as PnPpaperwt,sum(pnpadptrt) as pnpadptrt,sum(SizeOfOppcncwt) as SizeOfOppcncwt,sum(SizeOfOpppnpwt) as SizeOfOpppnpwt,sum(SelfServeCnCwt) as SelfServeCnCwt,avg(lnkadptrt) as lnkadptrt from dec3 group by providersyskey,healthcareorganizationname")
val decgrpovadp = decgrp.withColumn("overalladprt",(((col("lnkadptrt")*col("SizeOfOppcncwt")*3.59)/(col("SizeOfOppcncwt")*3.59+col("PnPpaperwt")*.5))+((col("pnpadptrt")*col("PnPpaperwt")*.5)/(col("SizeOfOppcncwt")*3.59+col("PnPpaperwt")*.5))))
decgrpovadp.createOrReplaceTempView("decgrpovadp")
val RP = spark.sql("select concat(date_format(add_months(from_unixtime(unix_timestamp()),-2),'yyyy-MM'),'-01') as date1,'XX' as temp,concat(date_format(add_months(from_unixtime(unix_timestamp()),-4),'yyyy-MM'),'-01') as date,* from decgrpovadp")
RP.createOrReplaceTempView("RP")
val RP1=spark.sql("select concat(date,temp,date1) as datenew,* from RP")
val RP2 = RP1.withColumn("newcol",expr("regexp_replace(datenew, '\\-', '')"))
val RP3 = RP2.withColumn("ReportingPeriod",expr("regexp_replace(newcol, 'XX', '-')"))
val RPFinal = RP3.drop("datenew","date1","temp","date","newcol")
val SSout = RPFinal.select(col("ProviderSysKey"),col("ProviderSystem"),col("ReportingPeriod"),col("avgpclmprctm"),col("avgpclmcnt"),col("avgeclmprctm"),col("avgeclmcnt"),col("clmprctmdiff"),col("avgprecprctm"),col("avgpreccnt"),col("avgerecprctm"),col("avgereccnt"),col("reconprctmdiff"),col("callcnt"),col("calltm"),col("authcallcnt"),col("eligcallcnt"),col("bencallcnt"),col("clmcallcnt"),col("callcost"),col("PnPelecwt"),col("PnPpaperwt"),col("pnpadptrt"),col("SizeOfOppcncwt"),col("SizeOfOpppnpwt"),col("SelfServeCnCwt"),col("lnkadptrt"),col("overalladprt"))
SSout.repartition(1).write.mode("overwrite").parquet("maprfs://".concat(outputselfservice))
LoggerApp.logger.info("Parquet File creation  for Sef Serice Metrics Ended at : " + DateApp.getDateTime())
 }
catch {
      case e: Exception => LoggerApp.logger.info(s"Exception while Creating Parquet file for Selfservice data" :+ e.printStackTrace())
    }
  }
}