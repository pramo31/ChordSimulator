package cs441.project.chord.config

import com.typesafe.config.Config
import cs441.project.chord.utils.ConfigReader
import org.apache.log4j.Logger

class SimulationConfig(val configFileName: String = Constants.simulatorConfig.toString) {

  val logger: Logger = Logger.getLogger(SimulationConfig.getClass)

  logger.info("Reading config file and extracting configuration variables")

  val config: Config = ConfigReader.readConfig(configFileName)

  val simulationTime: Int = config.getInt("SIMULATION_TIME")
  val timeMarks: String = config.getString("TIME_MARK")

  val numNodes: Int = config.getInt("NUM_NODES")
  val numUsers: Int = config.getInt("NUM_USERS")

  val requestRange: String = config.getString("MIN_MAX_REQUESTS")
  val minRequests: Int = Integer.parseInt(requestRange.split("-")(0))
  val maxRequests: Int = Integer.parseInt(requestRange.split("-")(1))

  val requestRatio: String = config.getString("READ_WRITE_RATIO")
  val readRequests: Int = Integer.parseInt(requestRatio.split(":")(0))
  val writeRequests: Int = Integer.parseInt(requestRatio.split(":")(1))
}

object SimulationConfig {
  val configObj = new SimulationConfig()
}
