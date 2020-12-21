package com.optum.providerdashboard.etl
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration
import sys.process._
import org.apache.hadoop.fs._;
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.storage.StorageLevel._
import java.io.File
import com.optum.providerdashboard.common.{GlobalContext,DateApp,LoggerApp,SparkMongoLoad,DecryptionPassword}
object ProviderSystemsIncrementalLoad {
def main(args: Array[String]): Unit = {
try
  {
val spark = GlobalContext.createSparkSession("Parquet file craetion for Provider Systems Lookup Table History Load")
LoggerApp.logger.info("Creation of Parquet file for Provider Systems Lookup Table Started at : " + DateApp.getDateTime())
val provsysfile = spark.sparkContext.getConf.get("spark.input.provsysfile")
val provsysloc = spark.sparkContext.getConf.get("spark.output.provsysloc")
val provsysloctemp = spark.sparkContext.getConf.get("spark.output.provsysloctemp")
val epnpcommontinsloc = spark.sparkContext.getConf.get("spark.output.epnpcommontinsloc")
val outprovsysfile = spark.sparkContext.getConf.get("spark.output.provsysfile")
val outprovsysfile_temp = spark.sparkContext.getConf.get("spark.output.provsysfile_temp")
val provsysdbname  = spark.sparkContext.getConf.get("spark.input.provsysdbname")
val provystblname  = spark.sparkContext.getConf.get("spark.input.provystblname")
val provarchdir = spark.sparkContext.getConf.get("spark.archive.provdir")
val inputfileprovarchdir = spark.sparkContext.getConf.get("spark.archive.fileprovdir")
val provkeyarchdir = spark.sparkContext.getConf.get("spark.archive.provkeyarchdir")
val date = java.time.LocalDate.now.toString()
val formatteddate = date.replace('-','_')
val conf = new Configuration()
val fs = FileSystem.get(conf)
val datetimestamp = spark.sql("select from_unixtime(unix_timestamp())").head().getString(0) 
val datetimestampsplit = datetimestamp.split(" ",2)
val datetimestamp1 = datetimestampsplit(0) 
val datetimestamp2 = datetimestampsplit(1) 
val finaldatetimestamp = datetimestamp1.replace('-','_')+"_"+datetimestamp2.replace(':','_')
val destdir = provsysloc
val destdir_temp = provsysloctemp
val mapdestdir = "/mapr".concat(destdir)
val mapdestdir_temp =  "/mapr".concat(destdir_temp)
val mapprovarchdir = "/mapr".concat(provarchdir)
val mapinputfileprovarchdir = "/mapr".concat(inputfileprovarchdir)
val mapprovsysfile = "/mapr".concat(provsysfile)
val formdir = mapprovarchdir+"myinsights_providers"+"_"+formatteddate
val maprlessformdir = formdir.substring(5)
val formdirtimestamp:String = mapprovarchdir+"myinsights_providers"+"_"+finaldatetimestamp
val mapoutprovsysfile = "/mapr".concat(outprovsysfile) 
val mapoutprovsysfile_temp = "/mapr".concat(outprovsysfile_temp)
val mapprovkeyarchdir = "/mapr".concat(provkeyarchdir)
val provkeyformdir = mapprovkeyarchdir+"myinsights_providerkeys"+"_"+formatteddate
val maprlessprovkeyformdir = provkeyformdir.substring(5)
val provkeyformdirtimestamp:String = mapprovkeyarchdir+"myinsights_providers"+"_"+finaldatetimestamp
val customSchema = StructType(Array(StructField("tin", LongType, true),StructField("mpin", LongType, true),StructField("tinname", StringType, true),StructField("healthcareorganizationname", StringType, true),StructField("health_system_id", StringType, true),StructField("pcorname", StringType, true),StructField("claimsthrudate", StringType, true)))
/*pededf datafranme stores existing lookup table providers data with skey*/
val pededf = spark.sql(s"""select * from $provsysdbname.$provystblname""");
pededf.createOrReplaceTempView("pededf")
/*pedcsv dataframe stores latest providers data without skey*/ 
val pedcsv = spark.read.format("csv").option("header","true").schema(customSchema).load(provsysfile)
pedcsv.createOrReplaceTempView("pedcsv") 
/*provnotindf: Finding the providers data from latest file which is not present in existing lookup table*/
val provnotindf = spark.sql("select * from pedcsv where upper(trim(pedcsv.healthcareorganizationname)) not in (select upper(trim(healthcareorganizationname)) from pededf)")
provnotindf.createOrReplaceTempView("provnotindf")
/*epnewtinsdf : Findout existing provider's new tins data and assign the skey to it from the lookup table*/
val epnewtinsdf = spark.sql("select pedcsv.tin,pedcsv.mpin,pedcsv.tinname,pedcsv.healthcareorganizationname,pededf.providersyskey as providersyskey,case when pededf.health_system_id is null and pedcsv.health_system_id is null then pededf.health_system_id when pededf.health_system_id is null and pedcsv.health_system_id is not null then pedcsv.health_system_id when pededf.health_system_id is not null and pedcsv.health_system_id is null then pededf.health_system_id when pededf.health_system_id is not null and pedcsv.health_system_id is not null then pedcsv.health_system_id else pededf.health_system_id end as health_system_id,pedcsv.pcorname,pedcsv.claimsthrudate from  pededf join pedcsv on upper(trim(pededf.healthcareorganizationname))=upper(trim(pedcsv.healthcareorganizationname))").dropDuplicates()
epnewtinsdf.createOrReplaceTempView("epnewtinsdf")
val dupepnewtinsdf = spark.sql("select * from epnewtinsdf where epnewtinsdf.tin in (select tin from epnewtinsdf group by tin having count(tin) > 1)")
dupepnewtinsdf.createOrReplaceTempView("dupepnewtinsdf")
val epnewduptinsdf = dupepnewtinsdf.select(col("tin"),col("tinname"),col("healthcareorganizationname")).withColumn("RejectReason",lit("Different TIN Names for Same TIN belonging to same provider"))
val uniqueepnewtinsdf = spark.sql("select * from epnewtinsdf where epnewtinsdf.tin not in (select dupepnewtinsdf.tin from dupepnewtinsdf)")
uniqueepnewtinsdf.createOrReplaceTempView("uniqueepnewtinsdf")
//val epnewtinsdfwithdate = spark.sql("select epnewtinsdf.tin,epnewtinsdf.mpin,epnewtinsdf.tinname,epnewtinsdf.healthcareorganizationname,epnewtinsdf.providersyskey,epnewtinsdf.health_system_id,case when length(tin) = 9 then concat(substr(tin,1,2),'-',substr(tin,3)) when length(tin) = 1 then concat(00,'-',000000,TIN) when length(tin) = 2 then concat(00,'-',00000,TIN) when length(tin) = 3 then concat(00,'-',0000,TIN) when length(tin) = 4 then concat(00,'-',000,TIN) when length(tin) = 5 then concat(00,'-',00,TIN) when length(tin) = 6 then concat(00,'-',0,TIN) when length(tin) = 7 then concat(00,'-',TIN) when length(tin) = 8 then concat(0,substr(TIN,1,1),'-',substr(TIN,2)) else tin end as formattedtin,epnewtinsdf.pcorname,epnewtinsdf.claimsthrudate,from_unixtime(unix_timestamp()) as refresh_date from epnewtinsdf")
val epnewtinsdfwithdate = spark.sql("select uniqueepnewtinsdf.tin,uniqueepnewtinsdf.mpin,uniqueepnewtinsdf.tinname,uniqueepnewtinsdf.healthcareorganizationname,uniqueepnewtinsdf.providersyskey,uniqueepnewtinsdf.health_system_id,case when length(tin) = 9 then concat(substr(tin,1,2),'-',substr(tin,3)) when length(tin) = 1 then concat(00,'-',000000,TIN) when length(tin) = 2 then concat(00,'-',00000,TIN) when length(tin) = 3 then concat(00,'-',0000,TIN) when length(tin) = 4 then concat(00,'-',000,TIN) when length(tin) = 5 then concat(00,'-',00,TIN) when length(tin) = 6 then concat(00,'-',0,TIN) when length(tin) = 7 then concat(00,'-',TIN) when length(tin) = 8 then concat(0,substr(TIN,1,1),'-',substr(TIN,2)) else tin end as formattedtin,uniqueepnewtinsdf.pcorname,uniqueepnewtinsdf.claimsthrudate,from_unixtime(unix_timestamp()) as refresh_date from uniqueepnewtinsdf")
epnewtinsdfwithdate.createOrReplaceTempView("epnewtinsdfwithdate")
val etindiffprovnew = spark.sql("select epnewtinsdfwithdate.tin,epnewtinsdfwithdate.mpin,epnewtinsdfwithdate.tinname,epnewtinsdfwithdate.healthcareorganizationname,epnewtinsdfwithdate.providersyskey,epnewtinsdfwithdate.health_system_id,epnewtinsdfwithdate.formattedtin,epnewtinsdfwithdate.pcorname,epnewtinsdfwithdate.claimsthrudate,epnewtinsdfwithdate.refresh_date from epnewtinsdfwithdate join pededf where epnewtinsdfwithdate.tin = pededf.tin and epnewtinsdfwithdate.healthcareorganizationname !=pededf.healthcareorganizationname")
val etindiffprovold = spark.sql("select pededf.tin,pededf.mpin,pededf.tinname,pededf.healthcareorganizationname,pededf.providersyskey,pededf.health_system_id,pededf.formattedtin,pededf.pcorname,pededf.claimsthrudate,pededf.refresh_date from epnewtinsdfwithdate join pededf where epnewtinsdfwithdate.tin = pededf.tin and epnewtinsdfwithdate.healthcareorganizationname !=pededf.healthcareorganizationname")
val etindiffprov = etindiffprovnew.union(etindiffprovold)
etindiffprovnew.createOrReplaceTempView("etindiffprovnew")
val uniqueepnewtinsdfwithdate = spark.sql("select * from epnewtinsdfwithdate where epnewtinsdfwithdate.tin not in(select etindiffprovnew.tin from etindiffprovnew)")
val etdifprvrs = etindiffprov.select(col("tin"),col("tinname"),col("healthcareorganizationname")).withColumn("RejectReason",lit("TIN Belongs to Different Providers"))
/*eptinsdf : Update remaining attributes in case existing providers and existing tins*/
val eptinsdf = spark.sql("select pededf.tin as tin,case when pededf.mpin is null and pedcsv.mpin is null then pededf.mpin when pededf.mpin is null and pedcsv.mpin is not null then pedcsv.mpin when pededf.mpin is not null and pedcsv.mpin is null then pededf.mpin when pededf.mpin is not null and pedcsv.mpin is not null then pedcsv.mpin else pededf.mpin end as mpin,case when pededf.tinname is null and pedcsv.tinname is null then pededf.tinname when pededf.tinname is null and pedcsv.tinname is not null then pedcsv.tinname when pededf.tinname is not null and pedcsv.tinname is null then pededf.tinname when pededf.tinname is not null and pedcsv.tinname is not null then pedcsv.tinname else pededf.tinname end as tinname,pededf.healthcareorganizationname as healthcareorganizationname,pededf.providersyskey as providersyskey,case when pededf.health_system_id is null and pedcsv.health_system_id is null then pededf.health_system_id when pededf.health_system_id is null and pedcsv.health_system_id is not null then pedcsv.health_system_id when pededf.health_system_id is not null and pedcsv.health_system_id is null then pededf.health_system_id when pededf.health_system_id is not null and pedcsv.health_system_id is not null then pedcsv.health_system_id else pededf.health_system_id end as health_system_id,formattedtin,case when pededf.pcorname is null and pedcsv.pcorname is null then pededf.pcorname when pededf.pcorname is null and pedcsv.pcorname is not null then pedcsv.pcorname when pededf.pcorname is not null and pedcsv.pcorname is null then pededf.pcorname when pededf.pcorname is not null and pedcsv.pcorname is not null then pedcsv.pcorname else pededf.pcorname end as pcorname,case when pededf.claimsthrudate is null and pedcsv.claimsthrudate is null then pededf.claimsthrudate when pededf.claimsthrudate is null and pedcsv.claimsthrudate is not null then pedcsv.claimsthrudate when pededf.claimsthrudate is not null and pedcsv.claimsthrudate is null then pededf.claimsthrudate when pededf.claimsthrudate is not null and pedcsv.claimsthrudate is not null then pedcsv.claimsthrudate else pededf.claimsthrudate end as claimsthrudate from  pededf join pedcsv on pededf.tin = pedcsv.tin").dropDuplicates()
eptinsdf.createOrReplaceTempView("eptinsdf")
val dupepetinsdf = spark.sql("select * from eptinsdf where eptinsdf.tin in (select tin from eptinsdf group by tin having count(tin) > 1)")
dupepetinsdf.createOrReplaceTempView("dupepetinsdf")
val etinsdupdf = dupepetinsdf.select(col("tin"),col("tinname"),col("healthcareorganizationname")).withColumn("RejectReason",lit("TIN Belongs to Different Providers"))
val eptinsdfwithdate = spark.sql("select *,from_unixtime(unix_timestamp()) as refresh_date from dupepetinsdf")
eptinsdfwithdate.createOrReplaceTempView("eptinsdfwithdate")
/*pedhchs: Find out the new provider names*/
val pedhchs = spark.sql("select distinct healthcareorganizationname from provnotindf")
/*maxnum: Finding out maximum skey form the existing providers data*/
val maxnum = pededf.agg(max("providersyskey")).head().getInt(0)
/*Add skey to the new providers*/
val pedpskey = pedhchs.withColumn("providersyskey", row_number().over(Window.orderBy("healthcareorganizationname"))+maxnum)
pedpskey.createOrReplaceTempView("pedpskey")
/* Assign skey to the new providers data*/
val nptinsdf = spark.sql("select pedcsv.tin,pedcsv.mpin,pedcsv.tinname,pedcsv.healthcareorganizationname,pedpskey.providersyskey as providersyskey,pedcsv.health_system_id,case when length(tin) = 9 then concat(substr(tin,1,2),'-',substr(tin,3)) when length(tin) = 1 then concat(00,'-',000000,TIN) when length(tin) = 2 then concat(00,'-',00000,TIN) when length(tin) = 3 then concat(00,'-',0000,TIN) when length(tin) = 4 then concat(00,'-',000,TIN) when length(tin) = 5 then concat(00,'-',00,TIN) when length(tin) = 6 then concat(00,'-',0,TIN) when length(tin) = 7 then concat(00,'-',TIN) when length(tin) = 8 then concat(0,substr(TIN,1,1),'-',substr(TIN,2)) else tin end as formattedtin,pedcsv.pcorname,pedcsv.claimsthrudate from pedpskey join pedcsv on upper(trim(pedpskey.healthcareorganizationname)) = trim(upper(pedcsv.healthcareorganizationname))")
nptinsdf.createOrReplaceTempView("nptinsdf")
/*dupnptinsdf: Finds out Duplicate tins in new providers data and writes the tins information to a location, if it has any*/
val dupnptinsdf = spark.sql("select * from nptinsdf where nptinsdf.tin in (select tin from nptinsdf group by tin having count(tin) > 1)")
dupnptinsdf.createOrReplaceTempView("dupnptinsdf")
val npduptinsdf = dupnptinsdf.select(col("tin"),col("tinname"),col("healthcareorganizationname")).withColumn("RejectReason",lit("Different TIN Names for Same TIN belonging to same provider"))
/*epnpcommontinsdf: Finds out Duplicate tins between existing and new providers data and writes the tins information to a location, if it has any*/
val uniquenptinsdf = spark.sql("select * from nptinsdf where nptinsdf.tin not in (select dupnptinsdf.tin from dupnptinsdf)")
uniquenptinsdf.createOrReplaceTempView("uniquenptinsdf")
val uniquenptinsdfwithdate = spark.sql("select *,from_unixtime(unix_timestamp()) as refresh_date from uniquenptinsdf")
val epdfdupnptinsdf = spark.sql("select tin,mpin,tinname,healthcareorganizationname,providersyskey,health_system_id,case when length(tin) = 9 then concat(substr(tin,1,2),'-',substr(tin,3)) when length(tin) = 1 then concat(00,'-',000000,TIN) when length(tin) = 2 then concat(00,'-',00000,TIN) when length(tin) = 3 then concat(00,'-',0000,TIN) when length(tin) = 4 then concat(00,'-',000,TIN) when length(tin) = 5 then concat(00,'-',00,TIN) when length(tin) = 6 then concat(00,'-',0,TIN) when length(tin) = 7 then concat(00,'-',TIN) when length(tin) = 8 then concat(0,substr(TIN,1,1),'-',substr(TIN,2)) else tin end as formattedtin,pcorname,claimsthrudate,from_unixtime(unix_timestamp()) as refresh_date from uniquenptinsdf where uniquenptinsdf.tin in (select tin from pededf)")
epdfdupnptinsdf.createOrReplaceTempView("epdfdupnptinsdf")
val epdfdupnptinsfilterdf = spark.sql("select * from pededf where pededf.tin in (select tin from epdfdupnptinsdf)")
val epnpcommontinsdf = epdfdupnptinsfilterdf.union(epdfdupnptinsdf)
val epnpduptinsdf = epnpcommontinsdf.select(col("tin"),col("tinname"),col("healthcareorganizationname")).withColumn("RejectReason",lit("TIN Belongs to Different Providers"))
val rejectedtinsdf = etdifprvrs.union(etinsdupdf).union(epnewduptinsdf).union(npduptinsdf).union(epnpduptinsdf)
rejectedtinsdf.orderBy(col("tin")).repartition(1).write.mode("overwrite").option("header","true").csv(epnpcommontinsloc)
val rejectfile = fs.globStatus(new Path(s"""$epnpcommontinsloc/part*"""))(0).getPath().getName();
fs.rename(new Path(s"""$epnpcommontinsloc/"""+rejectfile), new Path(s"""$epnpcommontinsloc/myInsights_Onboarded_ProviderSystem_RejectedTINS_$formatteddate"""+".csv"));
val finaluniquenptinsdf = spark.sql("select * from uniquenptinsdf where uniquenptinsdf.tin not in(select tin from epdfdupnptinsdf)")
finaluniquenptinsdf.createOrReplaceTempView("finaluniquenptinsdf")
val finaluniquenptinsdfwithdate = spark.sql("select tin,mpin,tinname,healthcareorganizationname,providersyskey,health_system_id,case when length(tin) = 9 then concat(substr(tin,1,2),'-',substr(tin,3)) when length(tin) = 1 then concat(00,'-',000000,TIN) when length(tin) = 2 then concat(00,'-',00000,TIN) when length(tin) = 3 then concat(00,'-',0000,TIN) when length(tin) = 4 then concat(00,'-',000,TIN) when length(tin) = 5 then concat(00,'-',00,TIN) when length(tin) = 6 then concat(00,'-',0,TIN) when length(tin) = 7 then concat(00,'-',TIN) when length(tin) = 8 then concat(0,substr(TIN,1,1),'-',substr(TIN,2)) else tin end as formattedtin,pcorname,claimsthrudate,from_unixtime(unix_timestamp()) as refresh_date from finaluniquenptinsdf")
/*Finaldf:union of existng providers data,existing provider's new tins data,existing providers data with updates,new providers data */
val Finaldf = uniqueepnewtinsdfwithdate.union(finaluniquenptinsdfwithdate).union(eptinsdfwithdate).union(pededf)
Finaldf.createOrReplaceTempView("Finaldf")
/*FFinaldf:Fetch the latest records based on refresh date*/
val FFinaldf = spark.sql("select t.tin,t.mpin,t.tinname,t.healthcareorganizationname,t.providersyskey,t.health_system_id,t.formattedtin,t.pcorname,t.claimsthrudate,t.refresh_date FROM (SELECT tin,mpin,tinname,healthcareorganizationname,providersyskey,health_system_id,formattedtin,pcorname,claimsthrudate,refresh_date,row_number() over (partition by tin,healthcareorganizationname order by unix_timestamp(refresh_date,'yyyy-MM-dd hh:mm:ss') desc) as rn from Finaldf) t WHERE t.rn <= 1")
/*Writes the final data to a temp location*/
FFinaldf.repartition(1).write.mode("overwrite").parquet(destdir_temp)
/*epsdf:Exiting Provider's key information*/
val epsdf = spark.read.option("header","true").csv(outprovsysfile)
epsdf.createOrReplaceTempView("epsdf")
/*npdate:New Provider's key,status information*/
val npdate = spark.sql("select distinct providersyskey,healthcareorganizationname,'NEW' as status,to_date(from_unixtime(unix_timestamp(current_timestamp(),'yyyy-MM-dd'))) as create_date,to_date(from_unixtime(unix_timestamp(current_timestamp(),'yyyy-MM-dd'))) as refresh_date from nptinsdf")
npdate.createOrReplaceTempView("npdate")
/*epdate:Existing Provider's key,revised status information*/
val epdate = spark.sql("select providersyskey,healthcareorganizationname,'ACTIVE' as status,create_date,from_unixtime(unix_timestamp(),'yyyy-MM-dd') as refresh_date from epsdf")
epdate.createOrReplaceTempView("epdate")
/*provderslist:Cumulative information of Provider's Key and Status information*/
val provderslist = spark.sql("select * from epdate union select * from npdate order by providersyskey")
provderslist.repartition(1).write.mode("overwrite").option("header","true").csv(outprovsysfile_temp)
val peddate = date.replace('-','_')
val sourcedir = outprovsysfile_temp
/*Renaming the part file name to myInsights_Onboarded_ProviderSystem_with_date*/
val file = fs.globStatus(new Path(s"""$sourcedir/part*"""))(0).getPath().getName();
fs.rename(new Path(s"""$sourcedir/"""+file), new Path(s"""$sourcedir/myInsights_Onboarded_ProviderSystem_$peddate"""+".csv"));
/*Archival of Exiting provider Keys data and renaming the temp directory name to actual directory*/
if(fs.exists(new org.apache.hadoop.fs.Path(outprovsysfile))==true&&fs.exists(new org.apache.hadoop.fs.Path(outprovsysfile_temp))==true)
{
if(fs.isDirectory(new org.apache.hadoop.fs.Path(outprovsysfile))==true&&fs.isDirectory(new org.apache.hadoop.fs.Path(outprovsysfile_temp))==true)
{
if(fs.exists(new org.apache.hadoop.fs.Path(maprlessprovkeyformdir))==true)
{ 
s"""mv $mapoutprovsysfile $provkeyformdirtimestamp"""!; 
s"""mv $mapoutprovsysfile_temp $mapoutprovsysfile"""!;
}else{ 
s"""mv $mapoutprovsysfile $provkeyformdir"""!;
s"""mv $mapoutprovsysfile_temp $mapoutprovsysfile"""!;
}
}
else
{
println(outprovsysfile+" or "+outprovsysfile_temp+" is not directory")  
}
}
else
{
println(outprovsysfile+" or "+outprovsysfile_temp+" does not exist")
}
/*Archival of Exiting providers data and renaming the temp directory name to actual directory*/
if(fs.exists(new org.apache.hadoop.fs.Path(destdir))==true&&fs.exists(new org.apache.hadoop.fs.Path(destdir_temp))==true)
{
if(fs.isDirectory(new org.apache.hadoop.fs.Path(destdir))==true&&fs.isDirectory(new org.apache.hadoop.fs.Path(destdir_temp))==true)
{
if(fs.exists(new org.apache.hadoop.fs.Path(maprlessformdir))==true)
{ 
s"""mv $mapdestdir $formdirtimestamp"""!; 
s"""mv $mapdestdir_temp $mapdestdir"""!;
}else{ 
s"""mv $mapdestdir $formdir"""!;
s"""mv $mapdestdir_temp $mapdestdir"""!;
}
}
else
{
println(destdir+" or "+destdir_temp+" is not directory")  
}
}
else
{
println(destdir+" or "+destdir_temp+" does not exist")
}
/* Moving new csv file to archive location and recreating the parent directory in inbound location*/
import java.nio.file.{Files, Path, StandardCopyOption}
Files.move(new File(mapprovsysfile).toPath, new File(mapinputfileprovarchdir+"myinsights_providersfile"+finaldatetimestamp).toPath, StandardCopyOption.ATOMIC_MOVE)
val dir = new File(mapprovsysfile)
if(!dir.exists()){dir.mkdir}
LoggerApp.logger.info("Creation of Parquet file for Provider Lookup Table Ended at : " + DateApp.getDateTime())
}
catch {
      case e: Exception => LoggerApp.logger.info(s"Exception while Creation of Parquet file for Provider Systems Lookup Table" :+ e.printStackTrace())
    }
  }  
}