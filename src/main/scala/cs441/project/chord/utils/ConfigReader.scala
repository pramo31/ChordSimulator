package cs441.project.chord.utils

import java.io.IOException

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

object ConfigReader {

  val logger: Logger = Logger.getLogger(ConfigReader.getClass)

  /**
   * Utility method to get configuration variables
   *
   * @param configFileName The name of the config file to fetch
   * @throws IOException Throws any Input or Output exception experienced
   * @return Config Obj with all the configuration values from resources folder and @configFileName
   */
  @throws[IOException]
  def readConfig(configFileName: String): Config = {
    logger.debug(String.format("Reading configuration file %s.", configFileName))
    ConfigFactory.load(configFileName)
  }
}