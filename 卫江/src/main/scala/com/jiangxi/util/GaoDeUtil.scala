package com.jiangxi.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

object GaoDeUtil {

  def getBusiness(longtitude:Double,lat:Double): String ={
    val buffer = ListBuffer[String]()
    val location= longtitude+","+lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?key=bb85016d408526f9438815d6f879cb5f&location="+location
    //发送http请求,返回响应内容，json字符串
    val json: String = HttpUtil.get(url)
    val jSONObject: JSONObject = JSON.parseObject(json)
    val status: Int = jSONObject.getIntValue("status")
    if(status == 0) return null
      else {
        val regecodeJson: JSONObject = jSONObject.getJSONObject("regeocode")  //	逆地理编码列表
        if(regecodeJson != null){
          val addressJson: JSONObject = regecodeJson.getJSONObject("addressComponent")  //地址元素列表
          if(addressJson != null) {
            val busiarray: JSONArray = addressJson.getJSONArray("businessAreas")  //经纬度所属商圈列表
            if(busiarray != null){
              for(item<-busiarray.toArray){
                if(item.isInstanceOf[JSONObject]){
                  val json = item.asInstanceOf[JSONObject]
                  buffer.append(json.getString("name"))
                }
              }
            }
          }
        }
      }
//字段参考    https://lbs.amap.com/api/webservice/guide/api/georegeo
    val str= buffer.mkString(",")
    str
    }
  //37.8679110000,112.5974490000
  def main(args: Array[String]): Unit = {
    //val str: String = getBusiness(116.310003,39.99195)
    val str: String = getBusiness(112.5974166626,37.8678880597)
    println(str)
  }

}
