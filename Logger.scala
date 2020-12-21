package com.optum.providerdashboard.common

import org.apache.log4j.Level

/**
 * Created by docker on 9/7/17.
 */
object Logger {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("ETLLogger")
  log.setLevel(Level.ALL)
}