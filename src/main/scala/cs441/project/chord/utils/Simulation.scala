package cs441.project.chord.utils

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import cs441.project.chord.config.{Constants, SimulationConfig}
import cs441.project.chord.core._
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

class Simulation {

  val logger: Logger = Logger.getLogger(classOf[Simulation])
  implicit val timeout: Timeout = Timeout(Constants.defaultTimeout)

  logger.info("Creating Chord Actor System and populating chord nodes using Akka Actors")
  // Create a Chord Actor System
  val chordSystem: ActorSystem = createActorSystem(Constants.chordActorSystem, ConfigFactory.parseString(Constants.logConfig))
  // Create Nodes (Actors) in the chord system
  val chordNodes: List[BigInt] = addNodesToChord(chordSystem)

  // Starting the Akka Http Web server
  val server = new WebServer
  server.startServer(chordSystem, chordNodes)
  logger.info("Server online at http://%s:%s".format(Constants.hostName, Constants.port))

  logger.info("Creating User Actor System and populating Users using Akka Actors")
  // Create a User Actor System
  val userSystem: ActorSystem = createActorSystem(Constants.usersActorSystem, ConfigFactory.parseString(Constants.logConfig))
  // Generate Users (Actors) and populate them into the User Actor System
  val users: List[String] = generateUsers(userSystem)

  logger.info("Fetching data using API from Movies database")
  // Fetch Data From Movies Database
  val dataList: ArrayBuffer[Array[String]] = DataFetcher.fetchData()
  val splitIndex: Int = dataList.size * 4 / 5
  val readData: ArrayBuffer[Array[String]] = dataList.slice(0, splitIndex)
  val writeData: ArrayBuffer[Array[String]] = dataList.slice(splitIndex, dataList.size - 1)

  logger.info("Writing initial setup data to the chord system")
  // Populate/Write Data to the nodes
  val initialWriteCounter = new AtomicInteger(0)
  writeInitialDataToChord(readData, chordSystem, chordNodes)

  /**
   * Utility Method to create an actor system
   *
   * @param systemName The name of the Actor System
   * @param logConfig  The logger config file for the Actor System
   * @return @ActorSystem - The created actor system
   */
  def createActorSystem(systemName: String, logConfig: Config): ActorSystem = {
    ActorSystem(systemName, logConfig)
  }

  /**
   * Utility method to create Actor Nodes and populate the chord Actor system
   *
   * @param system The Chord Actor System
   * @return List[String] : The list of actor User Id/Name
   */
  def addNodesToChord(system: ActorSystem): List[BigInt] = {
    val numNodes = SimulationConfig.configObj.numNodes
    val nodeList = new Array[BigInt](numNodes)

    // Add One node initially to the Chord System
    val firstNodeHash = CommonUtils.getHash(Constants.nodePrefix + 1)
    nodeList(0) = firstNodeHash
    logger.info("Node " + 1 + " Id : " + firstNodeHash)
    val firstNode = system.actorOf(Props(new Node(firstNodeHash)), firstNodeHash.toString)

    // Add the other nodes with the first node as the reference
    for (i <- 2 to numNodes) {
      val nodeHash = CommonUtils.getHash(Constants.nodePrefix + i)
      nodeList(i - 1) = nodeHash
      logger.info("Node " + i + " Id : " + nodeHash)
      val node = system.actorOf(Props(new Node(nodeHash)), nodeHash.toString)
      node ! Join(firstNode)

      // Sleep for 100 milliseconds after populating each node into the system to allow updates to be completed to finger tables, data etc.
      Thread.sleep(100)
    }
    nodeList.toList
  }

  /**
   * Utility method to create Actor Users and populate the Users Actor system
   *
   * @param system The User Actor System
   * @return List[BigInt] : The list of actor node hash/id/name
   */
  def generateUsers(system: ActorSystem): List[String] = {
    val numUsers = SimulationConfig.configObj.numUsers
    val userList = new Array[String](numUsers)
    for (i <- 1 to numUsers) {
      val userId = Constants.userPrefix + i
      logger.info("User with id %s generated and added to the User system".format(userId))
      userList(i - 1) = userId
      system.actorOf(Props(new User), userId)

      // Sleep for 50 milliseconds after populating each user into the system
      Thread.sleep(50)
    }
    userList.toList
  }

  /**
   * Utility method to pre populate the chord system
   *
   * @param dataList List of key value data to write to the chord system
   * @param system   The chord Actor system reference
   * @param nodeList The list of chord node/actor hash/id
   */
  def writeInitialDataToChord(dataList: ArrayBuffer[Array[String]], system: ActorSystem, nodeList: List[BigInt]): Unit = {
    dataList.foreach(data => {
      val key = data(0)
      val value = data(1)
      SimulationUtils.fetchRandomNode(system, nodeList) ! WriteKeyFindNode(key, value, 0)
      initialWriteCounter.addAndGet(1)
    })

    // Wait for 1 second to ensure all the data is written to the system
    Thread.sleep(1000)

    logger.info("Number of key value pairs written initially to the chord system during setup : %s".format(initialWriteCounter.get))
  }

  /**
   * Utility method to terminate all three Actor System (Chord, User and Web server)
   */
  def terminateSimulation(): Unit = {

    logger.info("Terminating Simulation..........")
    // Terminate The User Actor System
    userSystem.terminate

    // Terminate the Akka Http Web server
    server.stopServer()

    // Terminate The Chord Actor System
    chordSystem.terminate
  }
}