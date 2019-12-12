package com.cmcc.dmp.label

import ch.hsr.geohash.GeoHash
import com.cmcc.dmp.Utils.{HttpUtils, JSONutils, JedisPoolUtils, NumberUtils, SparkHelper}
import com.cmcc.dmp.label.AdvertisementLabel.getLabel
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * requirement  商圈标签
  *
  * @author zhangsl
  * @date 2019/12/9 20:15 
  * @version 1.0
  */
object businessLabel {

  val key = "e27e9e81c377a70755082a196c36b9c1"
  val radius = 1000

  /**
    * 根据 geohash 去redis查询
    * @param geohash
    * @return
    */
  def selectKey(geohash: String):String = {
    // 创建连接
    val pool = JedisPoolUtils.getJedisPool()
    // 查询数据
    val business = pool.get(geohash)
    JedisPoolUtils.releaseResource(pool)
    return business
  }

  /**
    * 高德地图查询数据
    * @param longitude
    * @param lat
    * @return
    */
  def getGaode(longitude: Double, lat: Double): String = {
    val businessJSON: String = HttpUtils.getHttp(longitude.toString,lat.toString,key,radius)
    val business: String = JSONutils.getJson(businessJSON)
     business
  }

  /**
    * 插入到 redis 库
    * @param geohash
    * @param business
    * @return
    */
  def insertRedis(geohash: String, business: String) = {
    val jedisPool = JedisPoolUtils.getJedisPool
    jedisPool.set(geohash,business)
    JedisPoolUtils.releaseResource(jedisPool)
  }

  /**
    * 获取商圈信息
    *
    * @param longitude
    * @param lat
    * @return
    */
  def getBusiness(longitude: Double, lat: Double):String = {
    // 获取key
    val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,longitude,8)
    // 去查询redis有没有key的信息
    var business = selectKey(geohash)
    // 判断redis中的数据是否存在 不存在请求高德
    if (business==null||business.length==0){
       business = getGaode(longitude,lat)
       if (business!="" && business!=null) {
          // 插入到redis
          insertRedis(geohash,business)
       }
    }
    business
  }

  /**
    * 商圈标签
    *
    * @param args  每行数据
    * @return list集合
    */
  def getBusinessLabelTest(args: Any*): List[(String,Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]

    val longitude = NumberUtils.doubleEmpty(row.getAs[String]("longitude"))
    val lat = NumberUtils.doubleEmpty(row.getAs[String]("lat"))
    val busines = getBusiness(longitude,lat)
    if (StringUtils.isNotBlank(busines)) {
      val str = busines.split(",")
      str.foreach(x=>{
        list:+=(x,1)
      })
    }
    list
  }

  def getBusinessLabel(appName: String, master: String, filePath: String): Unit = {
    // 获取sparkConf对象
    val conf: SparkConf = SparkHelper.getSparkConf(appName,master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkHelper.getSpark(conf)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 读取数据文件
    val dataFrame: DataFrame = spark.read.parquet(filePath)

    // 用户不为空
    val userIdOne =
      """
        |imei !='' or mac!='' or idfa != '' or openudid!='' or androidid!='' or
        |imeimd5 !='' or macmd5!='' or idfamd5 != '' or openudidmd5!='' or androididmd5!='' or
        |imeisha1 !='' or macsha1!='' or idfasha1 != '' or openudidsha1!='' or androididsha1!=''
      """.stripMargin

    //val userIdAndBusinLabel: Dataset[(String, String)] =
    dataFrame
        //.filter(userIdOne)
        .filter(data=>{
          val row = data.asInstanceOf[Row]
          val longitudeDouble: Double = NumberUtils.doubleEmpty(row.getAs[String]("longitude"))
          val latDouble: Double = NumberUtils.doubleEmpty(row.getAs[String]("lat"))
          longitudeDouble >=73 && longitudeDouble <= 135 && latDouble>=3 && latDouble<=54
        })
        .mapPartitions(_.map(data=>{
          val row = data.asInstanceOf[Row]
          val businessList = getBusinessLabelTest(row)
          businessList
        }))
      .filter(_.size>0)
    .rdd.foreach(println)





//      .map(data=>{
//        val row = data.asInstanceOf[Row]
//        val longitude = row.getAs[String]("longitude")
//        val lat = row.getAs[String]("lat")
//        HttpUtils.getHttp(longitude,lat,key,radius)
//      })

  }

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val master = "local[*]"
    //val filePath = "out/*.parquet"
    val filePath = "outAnli/*.parquet"
    getBusinessLabel(appName,master,filePath)
  }
}
