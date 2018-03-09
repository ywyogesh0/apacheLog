package com.maven.spark

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.DateFormat
import java.text.SimpleDateFormat

object LogEnums extends Enumeration {
  val DATE, TIME, METHOD, URL, PROTOCOL, VERSION, CODE = Value
}

object Utilities {

  /** Only ERROR messages get logged. */
  def setupLogging() {
    import org.apache.log4j.{ Level, Logger }

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Retrieves a regular expression Pattern for parsing Apache logs. */
  def apacheLogPattern(): Pattern = {
    // val ddd = "\\d{1,3}"
    // val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"

    val non_whitespace_character = "(\\S+)"

    val ip_host = non_whitespace_character
    val u1 = non_whitespace_character
    val u2 = non_whitespace_character

    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"

    val bytes = non_whitespace_character

    val regex = s"$ip_host $u1 $u2 $dateTime $request $status $bytes"
    Pattern.compile(regex)
  }

  def extractHourSlot(time: String): Int = {
    val timeArray = time.split(":")

    if (timeArray.size == 3) {

      val hour = timeArray(0).toInt

      if (hour >= 0 && hour < 6) 1
      else if (hour >= 6 && hour < 12) 2
      else if (hour >= 12 && hour < 18) 3
      else if (hour >= 18 && hour < 24) 4

      else 0

    } else 0
  }

  def converDateStringFormat(date: String): String = {

    val originalFormat = new SimpleDateFormat("dd/MMM/yyyy");
    val finalFormat = new SimpleDateFormat("yyyy-MM-dd");

    finalFormat.format(originalFormat.parse(date))
  }

  def extractLogParams(text: String, value: LogEnums.Value): String = value match {

    case LogEnums.DATE => {
      var dateArray = text.split(":")

      if (dateArray.size >= 1)
        dateArray(0).replace("[", "")
      else
        "NA"
    }

    case LogEnums.TIME => {
      var timeArray = text.split(":")

      if (timeArray.size >= 4)
        timeArray(1) + ":" + timeArray(2) + ":" + timeArray(3).split(" ")(0)
      else
        "NA"
    }

    case LogEnums.METHOD => {
      var methodArray = text.split(" ")

      if (methodArray.size >= 1)
        methodArray(0)
      else
        "NA"
    }

    case LogEnums.URL => {
      var urlArray = text.split(" ")

      if (urlArray.size >= 2)
        urlArray(1)
      else
        "NA"
    }

    case LogEnums.PROTOCOL => {
      var versionArray = text.split(" ")

      if (versionArray.size >= 3) {
        var httpArray = versionArray(2).split("/")

        if (httpArray.size >= 2)
          httpArray(0)
        else
          "0"

      } else
        "0"
    }

    case LogEnums.VERSION => {
      var versionArray = text.split(" ")

      if (versionArray.size >= 3) {
        var httpArray = versionArray(2).split("/")

        if (httpArray.size >= 2)
          httpArray(1)
        else
          "0"

      } else
        "0"
    }

  }
}