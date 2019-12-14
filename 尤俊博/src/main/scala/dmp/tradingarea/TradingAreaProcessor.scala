package dmp.tradingarea

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import dmp.traits.Processor
import dmp.util.ConfigHelper
import dmp.util.redis.JedisClient
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TradingAreaProcessor extends Processor{

  val gaoDeUrl: String = ConfigHelper.gaoDeUrl

  override def processor(spark: SparkSession): Unit = {
    //定义参数
    val sourceDataFile: String = ConfigHelper.adFormatData //输入路径
    //处理日志数据
    val sourceData: DataFrame = spark.read.parquet(sourceDataFile)

    //处理数据
    import spark.implicits._
    //数据过滤，计算geoHash,根据geoHash去重
    //sourceData.printSchema()
    val sourceGeoHashDF: Dataset[Row] = sourceData.filter(
      """
        |long !=''
      """.stripMargin)//.filter($"long_s" > 63 && $"long_s" < 136 && $"lat" > 3 && $"lat" < 54)
      .filter(
        """
          |lat!=''
        """.stripMargin)
      .filter(
        """
          |cast(long as float) >63
        """.stripMargin)
      .filter(
        """
          |cast(long as float) <136
        """.stripMargin)
      .filter(
        """
          |  cast(lat as float) > 3
        """.stripMargin)
      .filter(
        """
          |cast(lat as float) <54
        """.stripMargin)
      .rdd
      .map(row => {
        val longitude: String = row.getAs[String]("long")  //经度
        val latitude: String = row.getAs[String]("lat")   //维度
        val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(latitude.toFloat, longitude.toFloat, ConfigHelper.keyLength)
        val area: String = JedisClient.redisQuery(geoHash)
        (geoHash, longitude, latitude, area)
      }).toDF("geoHash", "long", "lat", "area")
      .dropDuplicates("geoHash")


    //找出redis数据库中不存在的需要调用高德API查询的geoHash
    val midRDD: RDD[String] = sourceGeoHashDF.filter(
      s"""
        |area is null
      """.stripMargin)
      .rdd
      .map { case Row(geoHash, longitude, latitude, area) =>
        s"$geoHash:$longitude,$latitude"
      }

    val resultRDD: RDD[(String, String)] = midRDD.map(item => (item, getBussinessArea(item)))

    //将数据库不存在的全部存入
    val finall: RDD[Unit] = resultRDD.filter(tuple => tuple._2 != null).map(item => {
      val geoHash: String = item._1
      val area: String = item._2
      JedisClient.redisInsertByBussiness(geoHash, area)
    })

    finall.collect()

  }

  //高德获取商圈信息
  def getBussinessArea(geoHAshAndLAL: String): String = {
    val strLAL: String = geoHAshAndLAL.split(":")(1)
    val url: String = s"$gaoDeUrl&location=${strLAL}"


    //定义client,调用高德API获取json串
    val client: HttpClient = new HttpClient
    val method: GetMethod = new GetMethod(url)
    val statusCode: Int = client.executeMethod(method)

    //解析json串
    val result: (String) = if (statusCode>=200 && statusCode<300){
      val jsonStr: String = method.getResponseBodyAsString
      val jsonObject: JSONObject = JSON.parseObject(jsonStr)
      val geoInfo: JSONObject = jsonObject.getJSONObject("regeocode")
      if(geoInfo == null){
        return null
      }else{
        val business = geoInfo.getJSONObject("addressComponent")
        if(business == null){
          return null
        } else{
          val tmpGeo = business
            .getJSONArray("businessAreas")
          if(tmpGeo == null){
            return null
          } else{
            val areas: String = tmpGeo
              .toArray.collect{
              case (x: JSONObject) => {
                val name: String = x.asInstanceOf[JSONObject].getString("name")
                if(name != null)
                  name
                else
                  null
              }

            }.mkString(",")
            println(areas)
            areas
          }
        }
      }
    }
    else throw new RuntimeException

    result

  }
}
