package cs441.project.chord

import java.util.concurrent.atomic.AtomicInteger

import akka.util.Timeout
import cs441.project.chord.config.{Constants, SimulationConfig}
import cs441.project.chord.utils._
import org.apache.log4j.Logger

object SimulationDriver {

  val logger: Logger = Logger.getLogger(SimulationDriver.getClass)
  implicit val timeout: Timeout = Timeout(Constants.defaultTimeout)

  def main(args: Array[String]): Unit = {

    println("Starting simulation : Check log file for more logs")
    logger.info("----------Starting Simulation----------")
    val start = System.currentTimeMillis()

    val simulationObject = new Simulation

    // Starting the simulation
    startSimulation(simulationObject)

    simulationObject.terminateSimulation()

    val end = System.currentTimeMillis()
    logger.info("Total run time : %s seconds".format((end - start) / 1000))

    logger.info("----------Simulation Ended----------")
    println("Simulation has ended")
  }

  /**
   * Entry method to start simulation once setup is completed
   *
   * @param simulationObject Simulation object
   */
  def startSimulation(simulationObject: Simulation): Unit = {

    val start = System.currentTimeMillis()

    val requiredSimulationTimeMinutes = SimulationConfig.configObj.simulationTime
    val timeMarks = SimulationConfig.configObj.timeMarks.split(",")

    // Divide the simulation into one minute each and iterate it as a part simulation
    for (time <- 1 to requiredSimulationTimeMinutes) {

      // Fetch the number of requests to be made in this minute
      val requests = CommonUtils.generateRandom(SimulationConfig.configObj.minRequests, SimulationConfig.configObj.maxRequests)
      logger.debug("Running %sth minute of the simulation. Need to make %s requests".format(time, requests))
      println("Requests to be generated : " + requests)
      val requestCounter = new AtomicInteger(0)
      val startMinute = System.currentTimeMillis()

      // Loop for a minute and keep generating requests
      while ((System.currentTimeMillis() - startMinute) / 1000 < Constants.simulationPartTime) {
        // Check if the number of request to be made is already made or not
        if (requestCounter.get < requests) {
          // If not generate a request
          SimulationUtils.generateRequest(simulationObject)
          requestCounter.addAndGet(1)
        }
      }

      logger.info("Completed %sth minute of the simulation. Completed %s requests".format(time, requestCounter.get))
      println("Completed %sth minute of the simulation. Completed %s requests".format(time, requestCounter.get))

      if (timeMarks.contains(time.toString)) {
        // If the current simulation is a part of the marked time, capture the global snapshot
        logger.info("Collecting global snapshot after completion  of %s minutes of simulation".format(time))
        SimulationUtils.captureGlobalSnapshot(time.toString, simulationObject)
      }
    }

    // Capture the snapshot after simulation ends
    SimulationUtils.captureGlobalSnapshot(requiredSimulationTimeMinutes.toString, simulationObject)

    // Capture snapshot chord system
    SimulationUtils.captureNodeDetails(simulationObject)

    val end = System.currentTimeMillis()
    logger.info("Effective simulation time including snapshot creation : %s seconds".format((end - start) / 1000))
  }
}