package com.optum.providerdashboard.common

/**
 * Created by docker on 9/7/17.
 */
object Exceptions {

  case class NoSuchPathException(private val message: String = "",
                                 private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

}
