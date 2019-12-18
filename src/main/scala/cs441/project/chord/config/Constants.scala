package cs441.project.chord.config

import scala.concurrent.duration._

object Constants extends Enumeration {

  type Constants = Value
  val simulatorConfig = "ChordSimulator"
  val parserConfig = "Parser"
  val chordActorSystem = "ChordSystem"
  val chordSystemNamingPrefix: String = "akka://" + chordActorSystem + "/user/"
  val nodePrefix = "N"

  val m: Int = 22 //math.ceil(math.log10(Integer.MAX_VALUE) / math.log10(2)).toInt - 1
  val totalSpace: Int = Math.pow(2, m).toInt

  val logConfig =
    """
      akka {
          loglevel = INFO
      }"""

  val usersActorSystem = "UserSystem"
  val userSystemNamingPrefix: String = "akka://" + usersActorSystem + "/user/"
  val userPrefix = "U"

  val chordSeverSystem = "ChordServer"

  val read = "read"
  val write = "write"

  val port = 8080
  val hostName = "localhost"

  val simulationPartTime = 60

  val dataNotFoundMessage = "Data Not Found"

  val nodeReadReqFieldName = "Read Requests Served"
  val nodeWriteReqFieldName = "Write Requests Served"
  val nodeHopsFieldName = "Hops"

  val userReadReqFieldName = "Read Requests"
  val userWriteReqFieldName = "Write Requests"

  val defaultTimeout: FiniteDuration = 10.seconds
}
