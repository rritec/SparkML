package com.optum.providerdashboard.etl
import com.optum.providerdashboard.common.{GlobalContext,DateApp,LoggerApp,SparkMongoLoad}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql.types._


object PCORExtract {
  def main(args: Array[String]): Unit = {
try
  {
val spark = GlobalContext.createSparkSession("Parquet File Creation for PCOR Data")
LoggerApp.logger.info("Parquet File Creation for PCOR Data Started at : " + DateApp.getDateTime())
val pcorin  = spark.sparkContext.getConf.get("spark.pcor.inputfile")
println("pcorin :" + pcorin)
val pcorout  = spark.sparkContext.getConf.get("spark.pcor.outputpath")
println("pcorout :" + pcorout)
val provdb  = spark.sparkContext.getConf.get("spark.pcor.provdab")
println("provdb :" + provdb)
val pvdtable  = spark.sparkContext.getConf.get("spark.pcor.pvdtable")
println("pvdtable :" + pvdtable)
val customSchema = StructType(Array(
        StructField("claims_thru_dt", StringType, true),
        StructField("Health_System_ID", StringType, true),
        StructField("Health_System_Name", StringType, true),
        StructField("Total_Provider_Group_Count", IntegerType, true),
		StructField("Total_Patient_Count", IntegerType, true),
		StructField("Total_Diabetic_Patients", IntegerType, true),
		StructField("Total_ACVs", IntegerType, true),
		StructField("Total_Diabetic_ACVs", IntegerType, true),
		StructField("Average_Star_Rating", DoubleType, true),
		StructField("1_Star_Measure_Count", IntegerType, true),
		StructField("2_Star_Measure_Count", IntegerType, true),
		StructField("3_Star_Measure_Count", IntegerType, true),
		StructField("4_Star_Measure_Count", IntegerType, true),
		StructField("5_Star_Measure_Count", IntegerType, true)))
val df1 = spark.read.format("csv").option("header","true")
             .schema(customSchema)
             .load(pcorin)
df1.createOrReplaceTempView("df1")
val source = spark.sql(s"""select p1.*,p2.providersyskey from df1 p1 join $provdb.$pvdtable p2 on p1.health_system_id = p2.health_system_id""");
val sourcedist = source.distinct
sourcedist.createOrReplaceTempView("sourcedist")
val df = spark.sql("select claims_thru_dt as ReportingPeriod,providersyskey as ProviderSysKey,Total_Provider_Group_Count as TotalProviderGroupCount,Total_Patient_Count as TotalPatientCount,Total_Diabetic_Patients as TotalDiabeticPatients,Total_ACVs as TotalACVs,Total_Diabetic_ACVs as TotalDiabeticACVs,Average_Star_Rating as AverageStarRating,1_Star_Measure_Count as 1StarMeasureCount,2_Star_Measure_Count as 2StarMeasureCount,3_Star_Measure_Count as 3StarMeasureCount,4_Star_Measure_Count as 4StarMeasureCount,5_Star_Measure_Count as 5StarMeasureCount from sourcedist") 
df.repartition(1).write.mode("overwrite").parquet(pcorout)
LoggerApp.logger.info("Parquet File Creation for PCOR Data Ended at : " + DateApp.getDateTime())
  }
catch
  {
case e: Exception => LoggerApp.logger.info(s"Exception while Parquet File creation for PCOR Data" :+ e.printStackTrace())
  }
}
}