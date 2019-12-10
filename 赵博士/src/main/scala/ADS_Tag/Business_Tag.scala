package ADS_Tag

import java.io.{PrintWriter, StringWriter}

import Spark_Utils.{Spark_Amap_Util, Spark_Redis_Util}
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/10
  * Time: 8:47
  *
  * @author zbs
  * @version 1.0
  */
object Business_Tag {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def makeTag(prelin: Seq[Any]) = {
    var list = List[(String, Int)]()
    var longs = prelin(22).toString
    var lats = prelin(23).toString
    var long: Double = 0.0
    var lat: Double = 0.0
    if (longs.nonEmpty || lats.nonEmpty) {
      long = longs.toDouble
      lat = lats.toDouble
      if (long <= 73 || long >= 135 || lat <= 3 || lat >= 54) {
        long = 0.0
        lat = 0.0
      }
    }
    if (long != 0.0 && lat != 0.0) {
      val business: String = makeBussiness(long, lat)
      if (business != null || StringUtils.isNoneBlank(business)) {
        val str = business.split(",")
        str.foreach(t => {
          list :+= (t, 1)
        })
      }
    }
    list

  }

  def makeBussiness(long: Double, lat: Double) = {
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
    var business: String = redisQuery(geoHash)
//    println(business)
    if (business == null || business.isEmpty) {
      business = Spark_Amap_Util.getBusinesss(long, lat)
      if (business!=null) {
        redisInsert(geoHash, business)
      }
    }
    business

  }

  /**
    * @Author zbs
    * @Description:查询redis数据库
    * @Date: 2019/12/10
    * @Param business:
    * @return: java.lang.String
    */
  def redisQuery(geohash: String) = {
    var bussiness: String = null
    try {
      val connect = Spark_Redis_Util.getConnect
      bussiness = connect.get(geohash)
      Spark_Redis_Util.releaseReource(connect)
    }
    catch {
      case ex: Exception => {
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        ex.printStackTrace(pw)
        logger.error(s"redis读取数据出错:\n${sw.toString}")
      }
    }
    bussiness
  }

  /**
    * @Author zbs
    * @Description:写入数据库
    * @Date: 2019/12/10
    * @Param geohash:
    * @Param business:
    * @return: java.lang.Object
    */
  def redisInsert(geohash: String, business: String) = {
    try {
      val jedis: Jedis = Spark_Redis_Util.getConnect
      jedis.set(geohash, business)
      Spark_Redis_Util.releaseReource(jedis)
    }
    catch {
      case ex: Exception => {
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        ex.printStackTrace(pw)
        logger.error(s"redis写入数据出错:\n${sw.toString}")
      }
    }
  }

}
