package samples

import org.junit._

import com.maven.spark.Utilities._
import com.maven.spark.LogEnums._

import scala.io.Source
import java.util.regex.Pattern
import java.util.regex.Matcher

@Ignore
@Test
class AppTest {

  @Test
  def testLogParsing() {

    val fileStream = getClass.getResourceAsStream("/access_log")
    val lines = Source.fromInputStream(fileStream).getLines

    val pattern = apacheLogPattern()

    // check for array size
    var schemaList: List[(String, String, String, String, String, String, String, Int, Int)] = List()

    for (line <- lines) {
      //val array: Array[String] = line.split(" ")

      val array = patternMatcher(line, pattern)

      var arraySize = array.size

      if (arraySize != 4)
        println(s"Error $arraySize")

      else {
        var ip_host = array(0)
        var date = extractLogParams(array(1), DATE)
        var time = extractLogParams(array(1), TIME)
        var method = extractLogParams(array(2), METHOD)
        var url = extractLogParams(array(2), URL)
        var protocol = extractLogParams(array(2), PROTOCOL)
        var version = extractLogParams(array(2), VERSION)
        var code = array(3).toInt
        var six_hour_slot = extractHourSlot(time)

        println("(" + ip_host + "," + converDateStringFormat(date) + "," + time + "," + method + "," + url + "," + protocol + "," + version + "," + code + "," + six_hour_slot + ")")

        schemaList = schemaList.+:((ip_host, date, time, method, url, protocol, version, code, six_hour_slot))
      }
    }

    // val newList = schemaList.diff(schemaList.distinct)
    // println("Duplicate List = " + newList)

    Assert.assertTrue(schemaList.size == 1546)
  }

  def patternMatcher(line: String, pattern: Pattern) = {
    val matcher = pattern.matcher(line)

    if (matcher.matches())
      Array(matcher.group(1), matcher.group(4), matcher.group(5), matcher.group(6))
    else
      Array("ERROR")
  }
}


