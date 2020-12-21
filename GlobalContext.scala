package com.optum.providerdashboard.common

/**
  * Created by smoham19 on 2/11/18.
  */
import org.apache.spark.sql.SparkSession


object  GlobalContext {


  val DEFAULT_DATA_FORMAT="yyyy-MM-dd"
  val DEFAULT_TIMESTAM_FORMAT="EEE MMM dd HH:mm:ss zzz yyyy"
  val RUN_MODE = List("PROD","TEST")

    def createSparkSession(appName: String): SparkSession = {
      val sparkSession = SparkSession.builder().appName(appName).enableHiveSupport()
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.binaryAsString","true")
      .config("hive.metastore.uris", "thrift://dbsls0306:11018")
      //.config("hive.metastore.uris", "thrift://dbsld0032:11075")
      //.config("hive.metastore.uris", "thrift://dbslp0307:11928")
      .getOrCreate()
    sparkSession.conf.set("spark.sql.crossJoin.enabled","true")
    val blockSize = 1024 * 1024 * 64     // 64MB
    sparkSession.sparkContext.hadoopConfiguration.setInt( "dfs.blocksize", blockSize )
    sparkSession.sparkContext.hadoopConfiguration.setInt( "parquet.block.size", blockSize )
    sparkSession
  }
 
}
