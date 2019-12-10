package util

import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}
import ch.hsr.geohash.GeoHash
import redis.clients.jedis.Jedis
import scalaj.http.{Http, HttpResponse}

import scala.util.parsing.json.JSON

object UDFUtil {

  private val stopWords = getValueFromOutsideFile()

  /**
    * Get stopWords Value From Outside File
    *
    * @return
    */
  def getValueFromOutsideFile() = {
    val ret = new ArrayBuffer[String]()

    //The Outside File may need to be upload to HDFS
    val source: BufferedSource = Source.fromFile("D:\\\\IdeaProjects\\\\sparkProjectTwo\\\\data\\\\sourceData\\\\stopWordsData\\\\stopwords.txt", "UTF-8")
    val stopWords: Iterator[String] = source.getLines()
    for (stopWord <- stopWords) {
      ret.+=(stopWord)
    }
    ret
  }

  /**
    * Transform Common Json From Option[Any] to Map[String,Any]
    *
    * @param json
    * @return
    */
  def transformCommonJson(json: Option[Any]) = json match {
    case Some(map: Map[String, Any]) => map
  }

  /**
    * Transform Json From Option[Any] to List[Map[String,String]]
    *
    * @param json
    * @return
    */
  def transformBusinessAreasJson(json: Option[Any]) = json match {
    case Some(map: List[Map[String, String]]) => map
  }

  /**
    * Get Trade Mark Tags From Json
    */
  def getTradeMarkTagsFromJson(httpResponseBody: String) = {
    var ret = ""

    val noTransformedAllJson: Option[Any] = JSON.parseFull(httpResponseBody)

    val transformedAllJson: Map[String, Any] = transformCommonJson(noTransformedAllJson)
    val transformedRegocodeJson: Map[String, Any] = transformCommonJson(transformedAllJson.get("regeocode"))
    val transformedAddressComponentJson: Map[String, Any] = transformCommonJson(transformedRegocodeJson.get("addressComponent"))

    if (transformedAddressComponentJson.get("businessAreas") != None) {
      val transformedBusinessAreasJson: List[Map[String, String]] = transformBusinessAreasJson(transformedAddressComponentJson.get("businessAreas"))

      if (transformedBusinessAreasJson != List(Nil)) {
        for (businessArea <- transformedBusinessAreasJson) {
          val businessAreaName: String = businessArea.get("name").get
          ret += businessAreaName + "|"
        }
      }
    }

    ret
  }

  /**
    * test
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println(getTradeMarkTags("116.310003", "46.991957"))
  }

  /**
    * Check if the Keyword is valid
    * return true when the Keywords is valid
    * return false Keywords is  not valid
    *
    * @param keyword
    * @return
    */
  def isValidKeyword(keyword: String) = {
    var ret: Boolean = true
    val keywordLen: Int = keyword.length

    if (keywordLen < 3 || keywordLen > 8)
      false
    else {
      for (stopWord <- stopWords) {
        if (keyword.contains(stopWord))
          ret = false
      }
      ret
    }
  }

  /**
    * Get Keywords Tag From Keywords
    *
    * @param keywords
    * @return
    */
  def getKeywordsTag(keywords: String) = {
    var res: String = ""
    val keywordsArray: Array[String] = keywords.split("\\|")
    for (i <- 0 to keywordsArray.length - 1) {
      if (isValidKeyword(keywordsArray(i))) {
        keywordsArray(i) = "K" + keywordsArray(i) + "|"
        res += keywordsArray(i)
      }
    }
    res
  }

  /**
    * Check if the Longitude and Latitude is valid
    *
    * @param longitude
    * @param latitude
    * @return
    */
  def isValidLongAndLat(longitude: String, latitude: String) = {
    if (longitude == "" || latitude == "") {
      false
    }
    else {
      val longitudeDouble: Double = longitude.toDouble
      val latitudeDouble: Double = latitude.toDouble

      if (longitudeDouble < 73.66 || longitudeDouble > 135.05 || latitudeDouble < 3.86 || latitudeDouble > 53.55) {
        false
      }
      else {
        true
      }
    }
  }

  /**
    * Get GeoHash From Longitude and Latitude
    *
    * @param longitude
    * @param latitude
    * @param precision
    */
  def getGeoHashFromLongAndLat(longitude: String, latitude: String, precision: Int) = {
    val longitudeDouble: Double = longitude.toDouble
    val latitudeDouble: Double = latitude.toDouble

    val geoHash: GeoHash = GeoHash.withCharacterPrecision(latitudeDouble, longitudeDouble, precision)
    geoHash.toBase32
  }

  //Can be Optimized By prefetching all the Trade Marks Before
  //Can be Optimized By updating the Trade Marks EveryDay
  def getTradeMarkTags(longitude: String, latitude: String) = {
    val geoHash = getGeoHashFromLongAndLat(longitude, latitude, 6)
    val jedis = new Jedis("hadoop01", 6379, 2000)
    var ret = ""

    val tradeMarkFromRedis: String = jedis.get(geoHash)
    if (tradeMarkFromRedis != null) {
      ret = tradeMarkFromRedis
    }
    else {
      //The Following Code Can be Optimized By using the Http Code (HTTP Status Code)
      //The Following Code Can Also be Optimized By using the Status Inside the Http Body
      val httpResponse: HttpResponse[String] = Http("https://restapi.amap.com/v3/geocode/regeo").params(Seq(("location", s"${longitude.toDouble},${latitude.toDouble}"), ("key", "6d371b3db8724b2f40aebac73d7e661b"))).asString

      //The Following Code is used for Limiting Access Per Second
      Thread.sleep(120)

      val httpResponseBody: String = httpResponse.body

      ret = getTradeMarkTagsFromJson(httpResponseBody)

      //The Following Code Can be Optimized By using the return of the jedis.set(geoHash,ret)
      jedis.set(geoHash, ret)
    }

    //May be Optimized By changing the position of the following One Line Code
    if (jedis != null) jedis.close()

    ret
  }
}
