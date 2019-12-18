package cs441.project.chord

import cs441.project.chord.config.Constants
import cs441.project.chord.core.WriteRequest
import cs441.project.chord.utils.Simulation
import org.scalatest._
import scalaj.http.Http

class ChordFunctionalityTest extends FunSuite {
  test("Chord System Node and User Count") {
    // Setup a base chord and user system (Basically the simulation setup) and verify if user and node is created
    // Setup with one node and a  user
    val simulationObject = new Simulation
    assert(simulationObject.chordNodes.size == 1)

    assert(simulationObject.users.size == 1)
    simulationObject.terminateSimulation()
  }
  test("Chord System Simulation for write data and read") {
    // Setup a base chord and user system
    // Write a key-value data and read that from the system. The response from the exposed web server should contain the value we wrote
    val simulationObject = new Simulation
    val key = "Test Movie"
    val value = "Releasing Today"
    val userId = simulationObject.users.head
    val user = simulationObject.userSystem.actorSelection(Constants.userSystemNamingPrefix + userId)

    // Write a key value into the chord system
    user ! WriteRequest(key, value)
    // Read the above written key value
    val read = Http("http://localhost:8080/chord").params(("title", key)).asString
    println(read.body)
    assert(read.body.contains(key) && read.body.contains(value))

    simulationObject.terminateSimulation()
  }
  test("Chord System Simulation read not existing data") {
    // Setup a base chord and user system
    // Write a key-value data but read a different (not present in the system) key.
    // The response from the exposed web server should contain 'not found' and should not contain the value.
    val simulationObject = new Simulation
    val key = "Test Movie"
    val value = "Releasing Today"
    val userId = simulationObject.users.head
    val user = simulationObject.userSystem.actorSelection(Constants.userSystemNamingPrefix + userId)

    val readInvalid = Http("http://localhost:8080/chord").params(("title", key)).asString
    println(readInvalid.body)
    assert(readInvalid.body.contains(key) && !readInvalid.body.contains(value) && readInvalid.body.contains("not found"))

    simulationObject.terminateSimulation()
  }
}