package com.optum.providerdashboard.etl
import com.optum.providerdashboard.common.{GlobalContext,DateApp,LoggerApp,SparkMongoLoad,DecryptionPassword}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.MongoSpark
object UserProviderAssociation {
def main(args: Array[String]): Unit = {
try
{
val spark = GlobalContext.createSparkSession("JSON Document Creation for UserProvider Association Collection")
LoggerApp.logger.info("JSON Document Creation for UserProvider Association Collection Started at : " + DateApp.getDateTime())
val mongodbname  = spark.sparkContext.getConf.get("spark.mongo.dbname")
println("mongodbname :" + mongodbname)
val mongohost  = spark.sparkContext.getConf.get("spark.mongo.host")
println("mongohost :" + mongohost)
val mongoport  = spark.sparkContext.getConf.get("spark.mongo.port").toInt
println("mongoport :" + mongoport)
val mongouname  = spark.sparkContext.getConf.get("spark.mongo.usernaame")
val mongocollection  = spark.sparkContext.getConf.get("spark.mongo.UserProviderAssociation")
//val mongopassword = spark.sparkContext.getConf.get("spark.mongo.password")
println("mongocollection :" + mongocollection)
val outputuserproviderassocloc = spark.sparkContext.getConf.get("spark.output.outputuserproviderassocloc")
val inputexcelfile = spark.sparkContext.getConf.get("spark.input.excelfile")
val passwordpath  = spark.sparkContext.getConf.get("spark.input.passwordpath")
val passphrase  = spark.sparkContext.getConf.get("spark.input.passphrase")
val command: String = "gpg --batch --passphrase "+passphrase+" --decrypt "+passwordpath;  
val mongopassword = DecryptionPassword.getPassword(command)
//val customschema = StructType(Array(StructField("ProviderKey", IntegerType, true),StructField("healthcareorganizationname", StringType, true),StructField("userID", StringType, true),StructField("roles", StringType, true)))
val customschema = StructType(Array(StructField("Providersyskey", IntegerType, true),StructField("Healthcareorganizationname", StringType, true),StructField("userID", StringType, true),StructField("roles", StringType, true)))
val provuserdf = spark.read.format("com.crealytics.spark.excel").option("useHeader", "true").schema(customschema).load(inputexcelfile)
val userroledf = provuserdf.withColumn("roles",array(col("roles")))
userroledf.repartition(1).write.mode("overwrite").json(outputuserproviderassocloc)
LoggerApp.logger.info("JSON Document Creation for UserProvider Association Collection Ended at : " + DateApp.getDateTime())  
LoggerApp.logger.info("Loading UserProvider Association data into Mongo Collection:UserProviderAssociation Started at : " + DateApp.getDateTime())
val loc = "maprfs://".concat(outputuserproviderassocloc)
SparkMongoLoad.MongoLoad(loc, mongodbname, mongocollection, mongohost, mongoport, mongouname, mongopassword)
LoggerApp.logger.info("Loading UserProvider Association data into Mongo Collection:UserProviderAssociation Ended at : " + DateApp.getDateTime())
}
catch {
      case e: Exception => LoggerApp.logger.info(s"Exception while JSON Document creation for UserProvider Association Collection" :+ e.printStackTrace())
    }
    }
}