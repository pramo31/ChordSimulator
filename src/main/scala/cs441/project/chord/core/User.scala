package cs441.project.chord.core

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Actor
import akka.util.Timeout
import com.google.gson.JsonObject
import cs441.project.chord.config.Constants
import org.apache.log4j.Logger
import scalaj.http.{Http, HttpResponse}

/**
 * A User Actor Class.
 * Contains logic to act as a user who does get and post request's about the movie data.
 */
class User() extends Actor {

  val logger: Logger = Logger.getLogger(classOf[User])

  implicit val timeout: Timeout = Timeout(Constants.defaultTimeout)

  val readRequests = new AtomicInteger()
  val writeRequests = new AtomicInteger()

  def receive: Receive = {

    /**
     * ReadRequest Message : The User actor does a http get request to the web server to fetch the data of release of the movie from the chord system
     */
    case ReadRequest(key: String) =>
      logger.info("Get Request : Release data for the movie %s.".format(key))
      val response: HttpResponse[String] = Http("http://localhost:8080/chord").params(("title", key)).asString
      readRequests.addAndGet(1)
      logger.info("Get Response : %s".format(response.body.toString))

    /**
     * WriteRequest Message : The User actor does a http put request to the web server to write the movie and data of release of the movie to the chord system
     */
    case WriteRequest(key: String, value: String) =>
      logger.info("Put Request : Insert %s with release date %s to database".format(key, value))
      val response = Http("http://localhost:8080/chord").params(("title", key), ("date", value)).method("PUT").asString
      writeRequests.addAndGet(1)
      logger.info("Put Response : %s".format(response.body.toString))

    /**
     * Fetches The User Actors' variables like read/write requests made by the user
     */
    case FetchUserSnapshot =>
      val snapShotJson: JsonObject = new JsonObject
      snapShotJson.addProperty(Constants.userReadReqFieldName, readRequests.get)
      snapShotJson.addProperty(Constants.userWriteReqFieldName, writeRequests.get)
      sender ! SnapshotDetails(snapShotJson)
  }
}