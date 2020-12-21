package com.optum.providerdashboard.common
import java.text.SimpleDateFormat
import java.util.Date
object DateApp {
  def getDateTime(): String = {
    
  val formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
	val date = new Date();
	s"$date"
  }
}
