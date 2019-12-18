package cs441.project.chord.utils

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSelection, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.google.gson.{GsonBuilder, JsonArray, JsonObject}
import cs441.project.chord.config.{Constants, SimulationConfig}
import cs441.project.chord.core._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await

object SimulationUtils {

  implicit val timeout: Timeout = Timeout(Constants.defaultTimeout)

  /**
   * Generate Read/Write requests from users to the Chord System
   *
   * @param simulationObject The Simulation Object reference
   */
  def generateRequest(simulationObject: Simulation): Unit = {
    if (CommonUtils.generateProbability() * 100 <= SimulationConfig.configObj.readRequests) {
      val data = fetchRandomData(Constants.read, simulationObject.readData, simulationObject.writeData)
      fetchRandomUser(simulationObject.userSystem, simulationObject.users) ! ReadRequest(data(0))
      Thread.sleep(15)
    } else {
      val data = fetchRandomData(Constants.write, simulationObject.readData, simulationObject.writeData)
      fetchRandomUser(simulationObject.userSystem, simulationObject.users) ! WriteRequest(data(0), data(1))
      Thread.sleep(15)
    }
  }

  /**
   * Utility method to fetch a random node actor from the existing users in the chord system
   *
   * @param chorSystem The Actor system representing chord system
   * @param nodeList   The list of node's name/id from which random value has to be selected
   * @return An ActorSelection object referring to a randomly retrieved node actor
   */
  def fetchRandomNode(chorSystem: ActorSystem, nodeList: List[BigInt]): ActorSelection = {
    val index = CommonUtils.generateRandom(0, nodeList.size - 1)
    val node = nodeList(index)
    chorSystem.actorSelection(Constants.chordSystemNamingPrefix + node)
  }

  /**
   * Utility method to fetch a random user actor from the existing users in the user system
   *
   * @param userSystem The Actor system representing users
   * @param userList   The list of user's name/id from which random value has to be selected
   * @return An ActorSelection object referring to a randomly retrieved user actor
   */
  def fetchRandomUser(userSystem: ActorSystem, userList: List[String]): ActorSelection = {
    val index = CommonUtils.generateRandom(0, userList.size - 1)
    val user = userList(index)
    userSystem.actorSelection(Constants.userSystemNamingPrefix + user)
  }


  /**
   * Fetches randomized data to either read or write based on input
   *
   * @param requestType The request type (read/write)
   * @param readData    The data list from which we pick data we want to read from the chord system
   * @param writeData   The data list from which we pick data to write to the chord system
   * @return The key-value pair as an Array
   */
  def fetchRandomData(requestType: String, readData: ArrayBuffer[Array[String]], writeData: ArrayBuffer[Array[String]]): Array[String] = {

    if (requestType.matches(Constants.read)) {
      val index = CommonUtils.generateRandom(0, readData.size - 1)
      readData(index)
    } else {
      val index = CommonUtils.generateRandom(0, writeData.size - 1)
      writeData(index)
    }
  }

  /**
   * Entry method to capture global snapshot of the user and actor system
   *
   * @param timeMark         The time mark of the simulation snapshot
   * @param simulationObject The Simulation Object reference
   */
  def captureGlobalSnapshot(timeMark: String, simulationObject: Simulation): Unit = {
    val gson = new GsonBuilder().setPrettyPrinting().create()
    val chordNodesSnapshot = new JsonObject()
    val usersSnapshot = new JsonObject()
    val globalSnapshot = new JsonObject()

    val totalChordReadReq = new AtomicInteger(0)
    val totalChordWriteReq = new AtomicInteger(0)
    val totalUserReadReq = new AtomicInteger(0)
    val totalUserWriteReq = new AtomicInteger(0)
    val totalHops = new AtomicInteger(0)

    for (elem <- simulationObject.chordNodes) {
      val snapJson = fetchSnapShotDetails(simulationObject.chordSystem, Constants.chordSystemNamingPrefix, elem.toString())
      totalChordReadReq.addAndGet(snapJson.get(Constants.nodeReadReqFieldName).getAsInt)
      totalChordWriteReq.addAndGet(snapJson.get(Constants.nodeWriteReqFieldName).getAsInt)
      totalHops.addAndGet(snapJson.get(Constants.nodeHopsFieldName).getAsInt)
      chordNodesSnapshot.add("Node %s".format(elem), snapJson)
    }
    CommonUtils.writeJson(gson.toJson(chordNodesSnapshot), "/Snapshot/%s/Nodes".format(timeMark))

    for (elem <- simulationObject.users) {
      val snapJson = fetchSnapShotDetails(simulationObject.userSystem, Constants.userSystemNamingPrefix, elem)
      totalUserReadReq.addAndGet(snapJson.get(Constants.userReadReqFieldName).getAsInt)
      totalUserWriteReq.addAndGet(snapJson.get(Constants.userWriteReqFieldName).getAsInt)
      usersSnapshot.add("User %s".format(elem), snapJson)
    }
    CommonUtils.writeJson(gson.toJson(usersSnapshot), "/Snapshot/%s/Users".format(timeMark))

    totalChordWriteReq.set(totalChordWriteReq.get - simulationObject.initialWriteCounter.get)
    val totalReqByUsers = totalUserReadReq.get + totalUserWriteReq.get
    val totalReqServerByChord = totalChordReadReq.get + totalChordWriteReq.get
    val successfulReq = totalReqServerByChord
    val failedReq = totalReqByUsers - totalReqServerByChord
    globalSnapshot.addProperty("User Read Requests", totalUserReadReq.get)
    globalSnapshot.addProperty("User Write Requests", totalUserWriteReq.get)
    globalSnapshot.addProperty("Node Read Requests Served", totalChordReadReq.get)
    globalSnapshot.addProperty("Node Write Requests Served", totalChordWriteReq.get)
    globalSnapshot.addProperty("Initial Write Requests", simulationObject.initialWriteCounter.get)
    globalSnapshot.addProperty("Successful Requests", successfulReq)
    globalSnapshot.addProperty("Failed Requests", failedReq)
    globalSnapshot.addProperty("Total Hops", totalHops.get)
    globalSnapshot.addProperty("Average Hops per request", 1.0 * totalHops.get / (totalReqByUsers + simulationObject.initialWriteCounter.get))
    globalSnapshot.addProperty("Nodes in Chord", simulationObject.chordNodes.size)
    CommonUtils.writeJson(gson.toJson(globalSnapshot), "/Snapshot/%s/Global".format(timeMark))
  }

  /**
   * Fetched the snapshot details of the Chord System details and writes the JSON to a file
   *
   * @param simulationObject The Simulation Object reference
   */
  def captureNodeDetails(simulationObject: Simulation): Unit = {
    val gson = new GsonBuilder().setPrettyPrinting().create()
    val nodeDetailArray = new JsonArray()
    for (elem <- simulationObject.chordNodes) {
      val node = simulationObject.chordSystem.actorSelection(Constants.chordSystemNamingPrefix + elem)
      val future = node ? FetchChordSystemData
      val nodeDetails = Await.result(future, timeout.duration).asInstanceOf[SnapshotDetails]
      val nodeJson = nodeDetails.nodeDetails
      nodeDetailArray.add(nodeJson)
    }
    val outputJsonArray = gson.toJson(nodeDetailArray)
    CommonUtils.writeJson(outputJsonArray, "ChordSystem")
  }

  /**
   * Fetched the snapshot details of the User/Actor details and writes the JSON to a file
   *
   * @param system             The Actor System
   * @param systemNamingPrefix The Actor System naming prefix
   * @param elem               The actor whose snapshot is to be fetched
   * @return
   */
  def fetchSnapShotDetails(system: ActorSystem, systemNamingPrefix: String, elem: String): JsonObject = {
    val node = system.actorSelection(systemNamingPrefix + elem)
    if (systemNamingPrefix.equals(Constants.userSystemNamingPrefix)) {
      val future = node ? FetchUserSnapshot
      val snapshotDetails = Await.result(future, timeout.duration).asInstanceOf[SnapshotDetails]
      snapshotDetails.nodeDetails
    } else {
      val future = node ? FetchNodeSnapshot
      val snapshotDetails = Await.result(future, timeout.duration).asInstanceOf[SnapshotDetails]
      snapshotDetails.nodeDetails
    }
  }
}
