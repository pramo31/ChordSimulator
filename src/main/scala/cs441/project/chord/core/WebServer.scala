
package cs441.project.chord.core

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import cs441.project.chord.SimulationDriver.timeout
import cs441.project.chord.config.Constants
import cs441.project.chord.utils.SimulationUtils
import org.apache.log4j.Logger

import scala.concurrent.{Await, ExecutionContextExecutor, Future}

/**
 * The Akka Http Web server
 * Exposes the GET and PUT web services
 */
class WebServer() {

  val logger: Logger = Logger.getLogger(classOf[WebServer])
  implicit val serverSystem: ActorSystem = ActorSystem(Constants.chordSeverSystem)
  var bindingFuture: Future[Http.ServerBinding] = _

  /**
   * Start the server and return the Future of Http.ServerBinding type which should be used to terminate the web ser
   *
   * @param chordSystem The chord Actor system
   * @param chordNodes  The list of chord actor node's names
   * @return
   */
  def startServer(chordSystem: ActorSystem, chordNodes: List[BigInt]): Unit = {

    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val route =
      path("chord") {
        concat(
          get {
            parameter("title".as[String]) { key =>
              val future = SimulationUtils.fetchRandomNode(chordSystem, chordNodes) ? FindKeyNode(key, 0)
              try {
                val keyValue = Await.result(future, timeout.duration).asInstanceOf[ReturnValue]
                if (!keyValue.value.equals(Constants.dataNotFoundMessage)) {
                  complete(StatusCodes.Accepted, "The movie %s was released in the year %s".format(key, keyValue.value))
                } else {
                  complete(StatusCodes.Accepted, "Sorry. Release date for the movie %s was not found".format(key))
                }
              } catch {
                case e: Exception =>
                  logger.error("Server is busy. Unable to serve request")
                  complete(StatusCodes.Accepted, "Sorry, the server is busy right now. Please try again later")
              }
            }
          }, put {
            parameter("title".as[String], "date".as[String]) { (key, value) =>
              SimulationUtils.fetchRandomNode(chordSystem, chordNodes) ! WriteKeyToNode(key, value, 0)
              complete(StatusCodes.Accepted, "The movie %s with its release date %s has been written to the database".format(key, value))
            }
          }
        )
      }

    bindingFuture = Http().bindAndHandle(route, Constants.hostName, Constants.port)
  }


  /**
   * Utility method to stop the Akka http server
   */
  def stopServer(): Unit = {
    implicit val executionContext: ExecutionContextExecutor = serverSystem.dispatcher
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => serverSystem.terminate()) // and shutdown when done
  }
}