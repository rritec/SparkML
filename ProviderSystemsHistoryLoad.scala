package com.optum.providerdashboard.etl
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._;
import org.apache.hadoop.fs.FileSystem
import com.optum.providerdashboard.common.{GlobalContext,DateApp,LoggerApp,SparkMongoLoad,DecryptionPassword}
object ProviderSystemsHistoryLoad {
def main(args: Array[String]): Unit = {
try
  {
val spark = GlobalContext.createSparkSession("Parquet file craetion for Provider Systems Lookup Table History Load")
val provsysfile = spark.sparkContext.getConf.get("spark.input.provsysfile")
val provsysloc = spark.sparkContext.getConf.get("spark.output.provsysloc")
val provsyscsvloc = spark.sparkContext.getConf.get("spark.output.outputprovsyscsvloc")
val provarchdir = spark.sparkContext.getConf.get("spark.archive.provdir")
val inputprovsysfile = "maprfs://".concat(provsysfile)
val outputprovsysloc = "maprfs://".concat(provsysloc)
val outputprovsyscsvloc = "maprfs://".concat(provsyscsvloc)
val mapprovsysfile = "/mapr".concat(provsysfile)
val mapprovarchdir = "/mapr".concat(provarchdir)
val date = java.time.LocalDate.now.toString()
val formatteddate = date.replace('-','_')
val conf = new Configuration()
val fs = FileSystem.get(conf)
val datetimestamp = spark.sql("select from_unixtime(unix_timestamp())").head().getString(0) 
val datetimestampsplit = datetimestamp.split(" ",2)
val datetimestamp1 = datetimestampsplit(0) 
val datetimestamp2 = datetimestampsplit(1) 
val finaldatetimestamp = datetimestamp1.replace('-','_')+"_"+datetimestamp2.replace(':','_')
LoggerApp.logger.info("Creation of Parquet file for Provider Systems Lookup Table Started at : " + DateApp.getDateTime())
/*Defining custom schema for the history load csv file*/
val customSchema = StructType(Array(StructField("tin", LongType, true),StructField("mpin", LongType, true),StructField("tinname", StringType, true),StructField("healthcareorganizationname", StringType, true),StructField("providersyskey", IntegerType, true),StructField("health_system_id", StringType, true),StructField("pcorname", StringType, true),StructField("claimsthrudate", StringType, true)))
/*Reading csv file as a datafrane*/
val pedcsv = spark.read.format("csv").option("header","true").schema(customSchema).load(inputprovsysfile)
pedcsv.createOrReplaceTempView("pedcsv")
/*Adding formattedtin fields which handles all tin lengths*/
val peddf =  spark.sql("select tin,mpin,trim(tinname) as tinname,trim(healthcareorganizationname) as healthcareorganizationname,providersyskey,health_system_id,case when length(tin) = 9 then concat(substr(tin,1,2),'-',substr(tin,3)) when length(tin) = 1 then concat(00,'-',000000,TIN) when length(tin) = 2 then concat(00,'-',00000,TIN) when length(tin) = 3 then concat(00,'-',0000,TIN) when length(tin) = 4 then concat(00,'-',000,TIN) when length(tin) = 5 then concat(00,'-',00,TIN) when length(tin) = 6 then concat(00,'-',0,TIN) when length(tin) = 7 then concat(00,'-',TIN) when length(tin) = 8 then concat(0,substr(TIN,1,1),'-',substr(TIN,2)) else tin end as formattedtin,pcorname,claimsthrudate,from_unixtime(unix_timestamp()) as refresh_date from pedcsv")
peddf.write.mode("overwrite").parquet(outputprovsysloc)
/*Selecting existing proviider name,provider keys*/
val pedhcpsk = peddf.select(col("providersyskey"),col("healthcareorganizationname")).distinct
pedhcpsk.createOrReplaceTempView("pedhcpsk")
/*Adding status,create_date and refresh_date fields*/
val existprovdf = spark.sql("select *,'ACTIVE' as status,'2018-07-31' as create_date,from_unixtime(unix_timestamp(),'yyyy-MM-dd') as refresh_date from pedhcpsk")
existprovdf.repartition(1).write.mode("overwrite").option("header","true").csv(outputprovsyscsvloc)
val peddate = date.replace('-','_')
val sourcedir = provsyscsvloc
val file = fs.globStatus(new Path(s"""$sourcedir/part*"""))(0).getPath().getName();
fs.rename(new Path(s"""$sourcedir/"""+file), new Path(s"""$sourcedir/myInsights_Onboarded_ProviderSystem_$peddate"""+".csv"));
import java.nio.file.{Files, Path, StandardCopyOption}
Files.move(new File(mapprovsysfile).toPath, new File(mapprovarchdir+"myinsights_providersfile"+finaldatetimestamp).toPath, StandardCopyOption.ATOMIC_MOVE)
val dir = new File(mapprovsysfile)
if(!dir.exists()){dir.mkdir}
LoggerApp.logger.info("Creation of Parquet file for Provider Lookup Table Ended at : " + DateApp.getDateTime())
}
catch {
      case e: Exception => LoggerApp.logger.info(s"Exception while Creation of Parquet file for Provider Systems Lookup Table" :+ e.printStackTrace())
    }
  }
}