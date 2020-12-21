package com.optum.providerdashboard.common
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
object DecryptionPassword {
val spark = GlobalContext.createSparkSession("Decryption of Mongo DB Password")

LoggerApp.logger.info("Decryption of Mongo DB Password Started at : " + DateApp.getDateTime())

def executeCommand( command:String):String = {
println("command in execute:::"+command)
 val p = Runtime.getRuntime.exec(command);
 val source = scala.io.Source.fromInputStream(p.getInputStream).getLines().mkString
  println("lines::::"+source)
  return source.toString();
}
	  
	  
	
   
    def getPassword(encrypted:String):String= {
       val shellOut = executeCommand(encrypted);
       val shellOutArr = shellOut.split("=");
       println(shellOutArr(0)+"::::"+shellOutArr(1))
var pwd = "";
       if (null != shellOutArr && shellOutArr.length == 2) {
             pwd = shellOutArr(1)
            println("Data Found")
             }
             else {
               print("no data found")
               }
             pwd          

}
}
