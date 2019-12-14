package com.jiangxi.tags

import ch.hsr.geohash.GeoHash
import com.jiangxi.`trait`.Tags
import com.jiangxi.entry.logBean
import com.jiangxi.util.{GaoDeUtil, JedisConnectionPool, TagsUtils}

object businessTag extends Tags{
  override def makeTag(args: Any*): List[(String, Double)] = {
    var list =List[(String,Double)]()
    if(args.length>0){
     //获取数据
      val log = args(0).asInstanceOf[logBean]
      //解析出字段
      val longtitude :Double = log.longitude.toDouble   //经度
      val lat: Double = log.lat.toDouble          //纬度
      //做判断，用户的经纬度是否是一个有效范围内的数据
      //  经度范围  73°33′E至135°05′E。纬度范围：3°51′N至53°33′N
       var business =""
      if(longtitude>=73 && longtitude <=135 && lat>=3 && lat<=53 ) {
        business = getBusiness(longtitude, lat)
        //根据geoHash串获取商圈信息，如果没有，发http请求=》高德=》商圈信息=》redis
        //打标签
        //println(longtitude+""+lat)
       if (business != null && ! business.isEmpty) {
          val businessArr: Array[String] = business.split(",")
          for (elem <- businessArr) {
            list :+= (elem, 1.0)
          }
        }
      }
    }
    list
  }

  //查看redis中的商圈信息
def getBusiness (longtitude:Double,lat:Double) ={
  // 获取Key
  val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,longtitude,6)
  // 第一步去查询数据库
  var business = redisQuery(geohash)
  // 第二步 如果数据库没有商圈 请求高德
  if(business == null || business.length ==0){
    business = GaoDeUtil.getBusiness(longtitude,lat)
    // 每次获取到新的商圈信息后，将新的商圈信息存入Redis中，进行缓存
    if(business != null && business.length >0 && business != ""){
      //  插入Redis
      redisInsertByBusiness(geohash,business)
    }
  }
  business
}

  //通过Redis数据库获取商圈
  def redisQuery(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }
  // 插入数据库
  def redisInsertByBusiness(geohash:String,business:String): Unit ={
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }

}
