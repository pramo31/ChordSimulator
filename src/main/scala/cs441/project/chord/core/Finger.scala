package cs441.project.chord.core

import akka.actor.ActorRef

/**
 * Finger Instance class representing each Finger node in the Finger table of the Chord Nodes
 *
 * @param start    Starting range of the hash (or the lowest value of hash that can be a valid finger)
 * @param node     The ActorRef of the Node Actor
 * @param nodeHash The Id/Hash of the Node
 */
class Finger(start: BigInt, var node: ActorRef, var nodeHash: BigInt) {

  def getStart: BigInt = {
    this.start
  }

  def getNode: ActorRef = {
    this.node
  }

  def getHash: BigInt = {
    nodeHash
  }

  def setNode(newNode: ActorRef): Unit = {
    this.node = newNode
  }

  def setHash(nodeHash: BigInt): Unit = {
    this.nodeHash = nodeHash
  }


  def metadata: String = {
    "Start: %s, Node: %s".format(getStart, getHash)
  }
}
