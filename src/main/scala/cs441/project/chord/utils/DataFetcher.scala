package cs441.project.chord.utils

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config
import cs441.project.chord.config.Constants
import org.apache.log4j.Logger
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import scalaj.http.{Http, HttpResponse}

import scala.collection.mutable.ArrayBuffer

object DataFetcher {

  val logger: Logger = Logger.getLogger(DataFetcher.getClass)

  /**
   * Utlity method to get the data to deal with in the chord system
   *
   * @return ArrayBuffer or Arr[String] : Returns an ArrayBuffer of Array containing Key and Value (Movie Title and Date Released)
   */
  def fetchData(): ArrayBuffer[Array[String]] = {
    println("Fetching data from API. This will take time due to constraints on request rate. Please wait.")

    val config: Config = ConfigReader.readConfig(Constants.parserConfig)
    implicit val formats: DefaultFormats.type = DefaultFormats
    val pageNumber = new AtomicInteger(1)
    val dataList = new ArrayBuffer[Array[String]]()

    while (pageNumber.get() <= config.getInt("parser.input.pages")) {

      val response: HttpResponse[String] = Http(s"https://api.themoviedb.org/3/discover/movie?api_key=${config.getString("parser.api.key")}&language=en-US&page=$pageNumber&primary_release_date.gte=${config.getString("parser.input.beginDate")}&primary_release_date.lte=${config.getString("parser.input.endDate")}").execute()

      try {
        val json = parse(response.body.toString)
        val elements = (json \ "results").children

        for (element <- elements) {
          val title = element \ "title"
          val year = element \ "release_date"

          dataList.addOne(Array(title.extract[String], year.extract[String]))
        }
      } catch {
        case exception: Exception => logger.error(exception)
      }

      if (pageNumber.get % 40 == 0) {
        println("Waiting for 8 seconds due to api requests limit")
        logger.debug("Waiting for 8 seconds due to api requests limit")
        Thread.sleep(8000)
      }

      pageNumber.addAndGet(1)
    }
    logger.debug("Fetched %s records from movie database".format(dataList.size))
    dataList
  }
}