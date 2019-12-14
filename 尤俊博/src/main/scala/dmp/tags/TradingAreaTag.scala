package dmp.tags

import ch.hsr.geohash.GeoHash
import dmp.traits.TagMaker
import dmp.util.ConfigHelper
import dmp.util.redis.JedisClient
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TradingAreaTag extends TagMaker{
  private val geoHashKeylength: Int = ConfigHelper.keyLength

  override def make(row: Row, dic: collection.Map[String, String]): Map[String, Double] = {
    val longitude: String = row.getAs[String]("long")
    val latitude: String = row.getAs[String]("lat")

    if(longitude == null || latitude == null || latitude.toFloat < 3 || latitude.toFloat >54 || longitude.toFloat < 63 || longitude.toFloat > 136){
      val map = Map[String, Double]("SQ无" -> 1.0)
      //map.foreach(println)
      map
    } else{
      val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(latitude.toFloat, longitude.toFloat, geoHashKeylength)

      //通过geoHash从redis数据库获取商圈信息
      val area: String = JedisClient.redisQuery(geoHash)
      if(area == null || area.length == 0 ) {
        val map = Map[String, Double]("SQ无" -> 1.0)
       // map.foreach(println)
        map
      } else{
        val map = area.split(",")
          //.filter(word => StringUtils.isNoneBlank(word))
          .map(word => "SQ" + word -> 1.0)
          .toMap
       // map.foreach(println)
        map
      }
    }
  }
}

