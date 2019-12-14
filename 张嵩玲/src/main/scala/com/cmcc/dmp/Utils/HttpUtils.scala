package com.cmcc.dmp.Utils

import java.net.URL

import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * requirement  http请求高德地图，获取Json串
  *
  * @author zhangsl
  * @date 2019/12/9 21:28 
  * @version 1.0
  */
object HttpUtils {

  def getUrl(longitude: String,lat: String,key:String,radius:Int,output:String = "json"):String = {
    output match {
      case "json" => return "https://restapi.amap.com/v3/geocode/regeo?" +
        "output=json&location="+longitude+","+lat+"&key="+key+"&radius="+radius+"&extensions=all"
      case "xml" => return "https://restapi.amap.com/v3/geocode/regeo?" +
        "output=xml&location="+longitude+","+lat+"&key="+key+"&radius="+radius+"&extensions=all"
      case _ => return null
    }

  }

  def getHttp(longitude: String,lat: String,key:String,radius:Int,output:String = "json"):String = {
    var client:CloseableHttpClient = null
    var response:CloseableHttpResponse = null

    // 获取Url
    try{
      val httpGet = new HttpGet(getUrl(longitude,lat,key,radius,output))
      client = HttpClients.createDefault()
      response = client.execute(httpGet)
      val entity: HttpEntity = response.getEntity
      val result = EntityUtils.toString(entity)
      return result
    }catch {
      case e : Exception =>{
        e.printStackTrace()
        return e.toString
      }
    }finally {
      if (response!=null){
        response.close()
      }
      if (client!=null){
        client.close()
      }
    }
  }

  // 63d91746f480d704a05e5c75b98a82a6
  def main(args: Array[String]): Unit = {
    println(getHttp("116.310003", "39.991957", "19741d341d31a6ee60f6969db3632c73", 1000))
  }
}
