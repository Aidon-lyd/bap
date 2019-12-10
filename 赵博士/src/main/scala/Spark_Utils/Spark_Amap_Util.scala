package Spark_Utils


import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/10
  * Time: 15:38
  *
  * @author zbs
  * @version 1.0
  */
object Spark_Amap_Util {
  val load = ConfigFactory.load("Amap.properties")
  private val logger: Logger = Logger.getLogger(this.getClass.getName)
  private val AMAP_KEY: String = load.getString("spark.amap.key")
  private val AMAP_URL: String = load.getString("spark.amap.url")
  def getBusinesss(long: Double, lat: Double): String = {
    val location = long + "," + lat
//    location=119.5493219955203,35.4054054054054&key=ba56367e3d8091937b560c5ba7032626&radius=1000&extensions=all
    val url = s"${AMAP_URL}location=${location}&key=${AMAP_KEY}"
    // 调用地图 发送http请求
    val json: String = Spark_Http_Util.get(url)
//    println("json"+json)
    // 解析json
    val jsonObject = JSON.parseObject(json)
    // 创建返回值集合
    val buffer = collection.mutable.ListBuffer[String]()
    // 获取状态码
    val status = jsonObject.getIntValue("status")
    // 判断状态码 1 正确 0 错误
    if (status == 0) return null
    // 如果不为空 进行取值操作
    val regeocodeJson = jsonObject.getJSONObject("regeocode")
    if (regeocodeJson == null) return null
    val addressComponent = regeocodeJson.getJSONObject("addressComponent")
    if (addressComponent == null) return null
    val businessAreas = addressComponent.getJSONArray("businessAreas")
    if (businessAreas == null) return null
    // 循环处理json内的数组
    for (i <- businessAreas.toArray) {
      if (i.isInstanceOf[JSONObject]) {
        val json = i.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }

}
