package com.optum.providerdashboard.common
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.MongoSpark

object SparkMongoLoad {
def MongoLoad(location:String,dbname:String,collectionname:String,host:String,port:Int,uname:String,password:String) = 
 {
  val spark = GlobalContext.createSparkSession("MongoLoad")
  val MSLoad = spark.read.json(location)
  MongoSpark.save(MSLoad.write.mode("overwrite").option("spark.mongodb.output.uri", s"""mongodb://$uname:$password@$host:$port/$dbname.$collectionname"""))
  }  
}
