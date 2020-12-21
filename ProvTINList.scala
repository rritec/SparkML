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

object ProvTINList {
  def main(args: Array[String]): Unit = {
try
  {
 val spark = GlobalContext.createSparkSession("JSON Document Creation for Provider-TIN List Collection")
LoggerApp.logger.info("JSON Documents creation  for Provider-TIN List Started at : " + DateApp.getDateTime())
val pvddbname  = spark.sparkContext.getConf.get("spark.provdbname")
println("pvddbname :" + pvddbname)
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

val mongocollection  = spark.sparkContext.getConf.get("spark.mongo.ProvTINList")
println("mongocollection :" + mongocollection)

val outputProvTINList = spark.sparkContext.getConf.get("spark.output.ProvTINList")
println("outputProvTINList :" + outputProvTINList)

println("ProvTINList - Table: " + System.currentTimeMillis())

val passwordpath  = spark.sparkContext.getConf.get("spark.input.passwordpath")
val passphrase  = spark.sparkContext.getConf.get("spark.input.passphrase")
val command: String = "gpg --batch  --passphrase "+passphrase+" --decrypt "+passwordpath;  
val mongopassword = DecryptionPassword.getPassword(command)
val d1 = spark.sql(s"""select providersyskey as ProviderKey,healthcareorganizationname as ProviderSystem,formattedtin as TIN,tinname as TINNAME from $pvddbname.$pedprvdsystbl group by providersyskey,healthcareorganizationname,formattedtin,tinname""")
d1.createOrReplaceTempView("d1")
val d2 = d1.withColumn("ALL",struct(col("TIN"),col("TINNAME")))
val d3 = d2.groupBy(col("ProviderKey"),col("ProviderSystem")).agg(collect_list(col("ALL")).as("ALL"))
d3.repartition(1).write.mode("overwrite").json("maprfs://".concat(outputProvTINList))
LoggerApp.logger.info("JSON Documents creation  for Provider-TIN List Ended at : " + DateApp.getDateTime())
LoggerApp.logger.info("Loading Provider-TIN List data into Mongo Collection:ProvTINList Started at : " + DateApp.getDateTime())
val loc = "maprfs://".concat(outputProvTINList)
SparkMongoLoad.MongoLoad(loc, mongodbname, mongocollection, mongohost, mongoport, mongouname, mongopassword)
LoggerApp.logger.info("Loading Provider-TIN List data into Mongo Collection:ProvTINList Ended at : " + DateApp.getDateTime())
  }
catch {
      case e: Exception => LoggerApp.logger.info(s"Exception while JSON Document creation for Provider-TIN List Collection" :+ e.printStackTrace())
    }
  }
}