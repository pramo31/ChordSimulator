package cs441.project.chord.utils

import java.io.File
import java.lang.Long
import java.security.MessageDigest

import cs441.project.chord.config.Constants
import org.apache.commons.io.FileUtils

import scala.util.Random

object CommonUtils {

  /**
   * Utility method to write a json file
   *
   * @param data     The json data to write
   * @param filePath Name of the json to be written
   */
  def writeJson(data: String, filePath: String): Unit = {
    val path = "output/%s.json".format(filePath)
    FileUtils.write(new File(path), data, "UTF-8")
  }

  /**
   * Utility method to fetch the hashed value. Hashed value should be consistent w.r.t the total space of chord(in bytes)
   *
   * @param id The value to be hashed
   * @return BigInt - The hashed value of @id
   */
  def getHash(id: String): BigInt = {

    // Maximum total hash space can be 2 ^ {m}.
    if (id != null) {
      var key = MessageDigest.getInstance("SHA-256").digest(id.getBytes("UTF-8")).map("%02X" format _).mkString.trim()
      if (key.length() > 15) {
        key = key.substring(key.length() - 15)
      }
      (Long.parseLong(key, 16) % Constants.totalSpace).toInt
    } else
      0
  }

  /**
   * Utility method to generate a random number between the given minimum and maximum values inclusive
   *
   * @param minimum The lower limit to the random number generated
   * @param maximum The upper limit to the random number generated
   * @return Integer - The random generated number within the range
   */
  def generateRandom(minimum: Int, maximum: Int): Int = {
    val rnd = new Random
    val randomNum = minimum + rnd.nextInt((maximum - minimum) + 1)
    randomNum
  }

  /**
   * Utility method to generate a random probability value
   *
   * @return The probability value between 0 to 1
   */
  def generateProbability(): Float = {
    val rnd = new Random
    val probability = rnd.nextFloat
    probability
  }


  /**
   * Utility method to validate whether a given value lies between a given range. The range here is circular.
   * Hence the left value need not always be lower than the right value.
   * If the left value is lower than the right value it indicates the other range of the circle.
   *
   * Example 1 -> @leftValue = 10 , @rightValue = 20 and @value = 15 returns true
   * Example 2 -> @leftValue = 20 , @rightValue = 10 and @value = 25 returns true
   * Example 3 -> @leftValue = 10 , @rightValue = 20 and @value = 25 returns false
   *
   * @param leftInclude  Boolean to indicate whether the left side value of range is inclusive
   * @param leftValue    The left side value of the range
   * @param rightValue   The right side value of the range
   * @param rightInclude Boolean to indicate whether the right side value of range is inclusive
   * @param value        The value which should be detected whether it lies within the range
   * @return boolean indicating whether the input @value lies within the given range parameters
   */
  def rangeValidator(leftInclude: Boolean, leftValue: BigInt, rightValue: BigInt, rightInclude: Boolean, value: BigInt): Boolean = {
    if (leftValue == rightValue) {
      true
    } else if (leftValue < rightValue) {
      if (value == leftValue && leftInclude || value == rightValue && rightInclude || (value > leftValue && value < rightValue)) {
        true
      } else {
        false
      }
    } else {
      if (value == leftValue && leftInclude || value == rightValue && rightInclude || (value > leftValue || value < rightValue)) {
        true
      } else {
        false
      }
    }
  }
}
