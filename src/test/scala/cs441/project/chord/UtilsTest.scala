package cs441.project.chord

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigException}
import cs441.project.chord.core.WebServer
import cs441.project.chord.utils.{CommonUtils, ConfigReader, DataFetcher}
import org.scalatest._
import scalaj.http.{Http, HttpResponse}

import scala.collection.mutable.ArrayBuffer

class UtilsTest extends FunSuite {

  test("The config reader reads the value from config file") {
    val config: Config = ConfigReader.readConfig("Test")
    val actual = config.getString("TestConfig")
    val expected = "TestValue"
    assert(actual == expected)
  }
  test("Invalid configName throws exceptions") {
    val config: Config = ConfigReader.readConfig("Invalid")
    assertThrows[ConfigException.Missing] {
      config.getString("InvalidConfigName")
    }
  }
  test("Read data from the movies API") {
    val dataList: ArrayBuffer[Array[String]] = DataFetcher.fetchData()
    val actual = dataList.size
    assert(actual > 0)
  }
  test("Range Validator 1") {
    val actual = CommonUtils.rangeValidator(leftInclude = false, 10, 20, rightInclude = false, 50)
    val expected = false
    assert(actual == expected)
  }
  test("Range Validator 2") {
    val actual = CommonUtils.rangeValidator(leftInclude = false, 20, 10, rightInclude = false, 50)
    val expected = true
    assert(actual == expected)
  }
  test("Range Validator 3") {
    val actual = CommonUtils.rangeValidator(leftInclude = false, 10, 20, rightInclude = false, 15)
    val expected = true
    assert(actual == expected)
  }
  test("Range Validator 4") {
    val actual = CommonUtils.rangeValidator(leftInclude = false, 10, 15, rightInclude = true, 15)
    val expected = true
    assert(actual == expected)
  }
  test("Range Validator 5") {
    val actual = CommonUtils.rangeValidator(leftInclude = false, 10, 10, rightInclude = false, 10)
    val expected = true
    assert(actual == expected)
  }
  test("Consistent Hashing") {
    val key = "KeyToHash"
    val expected = CommonUtils.getHash(key)
    for (i <- 1 to 100) {
      assert(expected == CommonUtils.getHash(key))
    }
  }
  test("Generate Random") {
    val start = 50
    val end = 100
    val result = CommonUtils.generateRandom(start, end)
    assert(start <= result)
    assert(result <= end)
  }
}
