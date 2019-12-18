package cs441.project.chord.core

import akka.actor.ActorRef
import com.google.gson.JsonObject

/**
 * Chord System Message Case Classes
 */
case class Join(nodeRef: ActorRef)

case class FindNodePosition(nodeRef: ActorRef, nodeHash: BigInt)

case class SuccessorAndPredecessor(predecessor: ActorRef, successor: ActorRef)

case class FindFingerNode(nodeRef: ActorRef, index: Int, start: BigInt)

case class FingerNodeRef(nodeRef: ActorRef)

case class UpdateFingerTables(previous: BigInt, index: Int, nodeRef: ActorRef, nodeHash: BigInt)

case class FindKeyNode(key: String, step: Int)

case class GetValueFromNode(key: String, step: Int)

case class ReturnValue(value: String)

case class WriteKeyFindNode(key: String, value: String, step: Int)

case class WriteKeyToNode(key: String, value: String, step: Int)

case class UpdateData()

case class AskData(keyHash: BigInt)

case class TellData(keyValue: Array[String])

case class SetPredecessor(nodeRef: ActorRef)

case class SetSuccessor(nodeRef: ActorRef)

case object Print

case object FetchNodeSnapshot

case object FetchChordSystemData

/**
 * User System Message Case Classes
 */
case class ReadRequest(key: String)

case class WriteRequest(key: String, value: String)

case object FetchUserSnapshot


/**
 * Commoon Message Case classes
 */
case class SnapshotDetails(nodeDetails: JsonObject)
