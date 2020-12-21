package com.optum.providerdashboard.common
import org.apache.log4j.Logger
import org.apache.log4j.Level
 import org.apache.log4j.PropertyConfigurator
object LoggerApp {
  @transient lazy val logger = org.apache.log4j.LogManager.getLogger("ClaimPassLogger");
   logger.setLevel(Level.ALL)
}
