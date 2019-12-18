package cs441.project.chord.core

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.google.gson.JsonObject
import cs441.project.chord.config.Constants
import cs441.project.chord.utils.CommonUtils
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.concurrent.Await

class Node(hashValue: BigInt) extends Actor {

  implicit val timeout: Timeout = Timeout(Constants.defaultTimeout)

  val logger: Logger = Logger.getLogger(classOf[Node])

  val readRequests = new AtomicInteger(0)
  val writeRequests = new AtomicInteger(0)
  val hops = new AtomicInteger(0)

  var existingNode: ActorRef = _
  var successor: ActorRef = self
  var predecessor: ActorRef = self

  var fingerTable = new Array[Finger](Constants.m)

  val data = new mutable.HashMap[BigInt, String]()

  // Initialize Finger table of size {m} with all finger nodes as self initially
  for (i <- 0 until Constants.m) {
    val start = (hashValue + BigInt(2).pow(i)) % BigInt(2).pow(Constants.m)
    fingerTable(i) = new Finger(start, self, hashValue)
  }


  /**
   * Find the closest preceding node of the Actor Node whose help has been asked by a New Joining Node / Key whose successor is to be located
   *
   * @param nodeHash The Id of the Node/Key whose successor we are trying to locate
   * @return ActorRef object which is the reference the closest/largest preceding node to the input @nodeHash
   */
  def closestPrecedingFinger(nodeHash: BigInt): ActorRef = {
    for (i <- Constants.m - 1 to 0 by -1) {
      if (CommonUtils.rangeValidator(leftInclude = false, hashValue, nodeHash, rightInclude = false, fingerTable(i).getHash)) {
        return fingerTable(i).node
      }
    }
    self
  }

  /**
   * Method to fetch the @index'th finger of the node in question(The new joining node)
   *
   * @param index The index of the finger table whose finger is to be set
   * @return Returns the Actor Reference who is the i'th finger node of the new joining node
   */
  def setFingerTable(index: Int): ActorRef = {
    var fingerRef: ActorRef = null

    // Check whether the start (or minimum) valid value of the index'th finger is in range of the node's hash and the previous finger's hash
    if (CommonUtils.rangeValidator(leftInclude = false, hashValue, fingerTable(index - 1).getHash, rightInclude = true, fingerTable(index).getStart)) {
      // If 'true' Then the previous index finger is also this index's finger
      fingerRef = fingerTable(index - 1).getNode
    } else {
      // If 'false' then we need to find the index'th finger node
      val future = existingNode ? FindFingerNode(self, index, fingerTable(index).getStart)
      val fingerNode = Await.result(future, timeout.duration).asInstanceOf[FingerNodeRef]
      fingerRef = fingerNode.nodeRef
    }
    fingerRef
  }

  /**
   * Entry method to notify all the nodes in the system about the new node's arrival and ask them to change their finger tables accordingly
   */
  def notifyAndUpdateOtherNodes(): Unit = {
    for (i <- 0 until Constants.m - 1) {
      val position = (hashValue - BigInt(2).pow(i) + BigInt(2).pow(Constants.m) + 1) % BigInt(2).pow(Constants.m)
      successor ! UpdateFingerTables(position, i, self, hashValue)
    }
  }

  /**
   * Entry method to update the nodes responsible for the data in the chord system once a new node joins the system
   */
  def updateDataReference(): Unit = {
    self ! UpdateData
  }

  /**
   * The Overriding Receive of the abstract Actor Method. This method captures received messages and process them.
   */
  def receive: Receive = {

    /**
     * Entry method for a new node. Joins the chord system, sets its successor, predecessor, finger table, notifies
     * other node's to update their finger tables and also updates the data it and it's successor is responsible for.
     */
    case Join(nodeRef: ActorRef) =>
      this.existingNode = nodeRef

      // Ask for successor and predecessor from the existing node
      val future = existingNode ? FindNodePosition(self, hashValue)
      val sucAndPreReference = Await.result(future, timeout.duration).asInstanceOf[SuccessorAndPredecessor]

      // Set the retrieved successor and predecessor
      this.predecessor = sucAndPreReference.predecessor
      this.successor = sucAndPreReference.successor

      // Notify predecessor to set this node as it's successor
      predecessor ! SetSuccessor(self)

      // Notify successor to set this node as it's predecessor
      successor ! SetPredecessor(self)

      // Update the Finger Table to the actual values from the default value
      // Set the successor to the 0th position on the finger table
      fingerTable(0).setNode(successor)
      fingerTable(0).setHash(successor.path.name.toInt)
      // Set the fingers of the other index
      for (i <- 1 until Constants.m) {
        val fingerRef = setFingerTable(i)
        fingerTable(i).setNode(fingerRef)
        fingerTable(i).setHash(fingerRef.path.name.toInt)
      }

      // Notify all the other nodes to update their Finger Tables
      notifyAndUpdateOtherNodes()

      // Update Data for self and successor node
      // updateDataReference()
      self ! UpdateData

    /**
     * Finds the position in the chord where the new node has to be placed by finding the position of the new node's prospective successor
     */
    case FindNodePosition(nodeRef: ActorRef, nodeHash: BigInt) =>
      // Check hash value of the input key is in range of the node's hash and the first finger's (successor) hash
      if (CommonUtils.rangeValidator(leftInclude = false, hashValue, fingerTable(0).getHash, rightInclude = true, nodeHash)) {
        // Found the responsible node to carry the data, send the Actor reference back to the sender
        sender ! SuccessorAndPredecessor(self, fingerTable(0).getNode)
      } else {
        //Find the 'closest_preceding_finger' and ask it to find the place of this new node
        val target = closestPrecedingFinger(nodeHash)
        val future = target ? FindNodePosition(nodeRef, nodeHash)
        val sucAndPreReference = Await.result(future, timeout.duration).asInstanceOf[SuccessorAndPredecessor]
        // Found the responsible node to carry the data, send the Actor reference back to the sender
        sender ! SuccessorAndPredecessor(sucAndPreReference.predecessor, sucAndPreReference.successor)
      }


    case FindFingerNode(node: ActorRef, index: Int, start: BigInt) =>
      // Check hash value of the input key is in range of the node's hash and the first finger's (successor) hash
      if (CommonUtils.rangeValidator(leftInclude = false, hashValue, fingerTable(0).getHash, rightInclude = true, start)) {
        // Found the node which is the finger of the calling node at the index'th position. Send the actor's reference to the seder
        sender ! FingerNodeRef(fingerTable(0).getNode)
      } else {
        //Find the 'closest_preceding_finger' and ask it to find the closest node ahead of the starting range of the index'th position in the finger table
        val target = closestPrecedingFinger(start)
        val future = target ? FindFingerNode(node, index, start)
        val fingerNode = Await.result(future, timeout.duration).asInstanceOf[FingerNodeRef]
        sender ! FingerNodeRef(fingerNode.nodeRef)
      }

    /**
     * Update the finger tables of the existing nodes w.r.t the newly joined node
     */
    case UpdateFingerTables(previous: BigInt, index: Int, nodeRef: ActorRef, nodeHash: BigInt) =>
      if (nodeRef != self) { // new node is not its own successor (usually happens only when there is only one node in chord)
        // Check if the hash position determined is in the range of the calling (The successor's) has and it's successor's hash
        if (CommonUtils.rangeValidator(leftInclude = false, hashValue, fingerTable(0).getHash, rightInclude = true, previous)) { //I am the node just before N-2^i
          // Check if the hash position determined is in the range of the calling (The successor's) has and it's index'th finger's hash
          if (CommonUtils.rangeValidator(leftInclude = false, hashValue, fingerTable(index).getHash, rightInclude = false, nodeHash)) {
            // Update the finger table of the node
            fingerTable(index).setNode(nodeRef)
            fingerTable(index).setHash(nodeHash)
            // Notify the predecessor to update its index'th position in the finger table if required
            predecessor ! UpdateFingerTables(hashValue, index, nodeRef, nodeHash)
          }
        } else {
          // Find the closest preceding finger and ask it to update the finger tables for the particular node
          val target = closestPrecedingFinger(previous)
          target ! UpdateFingerTables(previous, index, nodeRef, nodeHash)
        }
      }

    /**
     * Finding the node responsible for the input key, so that the value can be fetched and returned
     */
    case FindKeyNode(key: String, step: Int) =>
      def id: BigInt = CommonUtils.getHash(key)
      // Check hash value of the input key is in range of the predecessor's and the node's hash
      if (CommonUtils.rangeValidator(leftInclude = false, predecessor.path.name.toInt, hashValue, rightInclude = true, id)) {
        // This node itself is the owner of the key. Hence fetch the value if existing and return it
        logger.info("Key %s with hash: %s is owned by node %s. Found using %s steps".format(key, CommonUtils.getHash(key), self.path.name, step))
        readRequests.addAndGet(1)
        if (data.contains(CommonUtils.getHash(key))) {
          sender ! ReturnValue(data(CommonUtils.getHash(key)))
        } else {
          sender ! ReturnValue(Constants.dataNotFoundMessage)
        }
      } else {
        // Check hash value of the input key is in range of the node's hash and the first finger's (successor) hash
        if (CommonUtils.rangeValidator(leftInclude = false, hashValue, fingerTable(0).getHash, rightInclude = true, id)) {
          // Found the responsible node to carry the data, read the data from that node
          val future = fingerTable(0).getNode ? GetValueFromNode(key, step)
          val keyValue = Await.result(future, timeout.duration).asInstanceOf[ReturnValue]
          sender ! ReturnValue(keyValue.value)
        } else {
          // Find the 'closest_preceding_finger' and ask it to find the node responsible for the input key
          val target = closestPrecedingFinger(id)
          hops.addAndGet(1)
          val future = target ? FindKeyNode(key, step + 1)
          val keyValue = Await.result(future, timeout.duration).asInstanceOf[ReturnValue]
          // Once the key is fetched, return it to the sender of this message
          sender ! ReturnValue(keyValue.value)
        }
      }

    /**
     * Read the value of input key from the node once it is determined to be the node responsible for the data
     */
    case GetValueFromNode(key: String, step: Int) =>

      logger.info("Key %s with hash: %s is owned by node %s. Found using %s steps".format(key, CommonUtils.getHash(key), self.path.name, step))
      readRequests.addAndGet(1)
      if (data.contains(CommonUtils.getHash(key))) {
        sender ! ReturnValue(data(CommonUtils.getHash(key)))
      } else {
        sender ! ReturnValue(Constants.dataNotFoundMessage)
      }

    /**
     * Finding the node responsible for storing he input key-value pair
     */
    case WriteKeyFindNode(key: String, value: String, step: Int) =>
      def id: BigInt = CommonUtils.getHash(key)
      // Check hash value of the input key is in range of the node's hash and the first finger's (successor) hash
      if (CommonUtils.rangeValidator(leftInclude = false, hashValue, fingerTable(0).getHash, rightInclude = true, id)) {
        // Found the responsible node to carry the data, write the data to that node
        fingerTable(0).getNode ! WriteKeyToNode(key, value, step)
      } else {
        // Find the 'closest_preceding_finger' and ask it to find the node responsible for the input key
        val target = closestPrecedingFinger(id)
        hops.addAndGet(1)
        target ! WriteKeyFindNode(key, value, step + 1)
      }

    /**
     * Write/Update a particular key value to a node once it is determined to be the node responsible for the data
     */
    case WriteKeyToNode(key: String, value: String, step: Int) =>
      logger.info("Key %s with hash: %s should be owned owned by node %s. Found using %s steps. Writing data to this node".format(key, CommonUtils.getHash(key), self.path.name, step))
      writeRequests.addAndGet(1)
      if (data.contains(CommonUtils.getHash(key))) {
        data.update(CommonUtils.getHash(key), value)
      } else {
        data.addOne(CommonUtils.getHash(key), value)
      }

    /**
     * Update the new nodes data store by fetching the key value it is responsible for from its successor
     */
    case UpdateData =>
      val keyHash = new AtomicInteger(predecessor.path.name.toInt + 1)
      while (keyHash.get <= hashValue) {

        // Send the key (starting from lowest and incrementing till the self hash value) which the new node is
        // responsible for to its successor to check if there is any data and ask the successor to Tell the key value if any present
        val future = successor ? AskData(keyHash.get)
        val dataPart = Await.result(future, timeout.duration).asInstanceOf[TellData]
        if (dataPart.keyValue.length != 0) {

          // Received the key value pair which the node is now responsible for and
          // increment the hash value of the received key of the key value pair by one
          val key = dataPart.keyValue(0).toInt
          val value = dataPart.keyValue(1)
          data.addOne(key, value)
          keyHash.set(key + 1)
        } else {
          // If empty array is sent by successor, it indicates end of transaction. Set condition to terminate loop
          keyHash.set(hashValue.toInt + 1)
        }
      }


    /**
     * The sender actor node (which is a new/joining node) asks its successor to give the reference o
     * of all the keys that the new node is responsible for maintaining
     */
    case AskData(keyHash: BigInt) =>
      val count = new AtomicInteger(0)
      for (hash <- keyHash to predecessor.path.name.toInt) {
        if (data.contains(hash)) {
          val value = data(hash)
          count.addAndGet(1)

          // Remove the key and Tell the sender that the sender is responsible for the particular key now
          data.remove(hash)
          sender ! TellData(Array(hash.toString(), value))
        }
      }

      // No more keys which the new should be aware of. Send back an empty array to indicate end of transaction
      if (count.get() < 1) {
        sender() ! TellData(Array())
      }

    /**
     * Set the predecessor of the sender node as the input @nodeRef (which will be the new node joining the chord)
     */
    case SetPredecessor(nodeRef: ActorRef) =>
      this.predecessor = nodeRef

    /**
     * Set the successor of the sender actor node as the input @nodeRef (which will be the new node joining the chord)
     * Also set the @nodeRef as the first node in Finger table of the sender actor node
     */
    case SetSuccessor(nodeRef: ActorRef) =>
      this.successor = nodeRef
      fingerTable(0).setNode(nodeRef)
      fingerTable(0).setHash(nodeRef.path.name.toInt)

    /**
     * Print Actor's variable state
     */
    case Print =>
      println("============================================")
      println("NodeId: %s".format(context.actorSelection(Constants.chordSystemNamingPrefix + hashValue).pathString))
      println("Hash: %s".format(hashValue))
      println("Predecessor: %s".format(predecessor.path.name))
      println("Successor: %s".format(successor.path.name))
      println("Finger Table: ")
      for (i <- 0 until Constants.m) {
        println("   %d : ".format(i) + fingerTable(i).metadata)
      }
      println("Data Size Within : %s".format(data.size))
      data.foreach(keyValue => {
        println("   %d : %s".format(keyValue._1, keyValue._2))
      })
      println("============================================")

    /**
     * Fetches the Node's variables like read and write requests
     */
    case FetchNodeSnapshot =>
      val snapShotJson: JsonObject = new JsonObject
      snapShotJson.addProperty(Constants.nodeReadReqFieldName, readRequests.get)
      snapShotJson.addProperty(Constants.nodeWriteReqFieldName, writeRequests.get)
      snapShotJson.addProperty(Constants.nodeHopsFieldName, hops.get)
      sender ! SnapshotDetails(snapShotJson)

    /**
     * Fetches all the Node system's variable
     */
    case FetchChordSystemData =>
      val fingerJson: JsonObject = new JsonObject
      for (i <- fingerTable.indices) {
        fingerJson.addProperty(i.toString, fingerTable(i).getHash)
      }
      val nodeJson: JsonObject = new JsonObject
      nodeJson.addProperty("Node", hashValue)
      nodeJson.addProperty("Successor", successor.path.name)
      nodeJson.addProperty("Predecessor", predecessor.path.name)
      nodeJson.addProperty("KeyValuePairs", data.size)
      nodeJson.add("Fingers", fingerJson)
      sender ! SnapshotDetails(nodeJson)
  }
}