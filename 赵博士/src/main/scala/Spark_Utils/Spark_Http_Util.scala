package Spark_Utils

import java.io.{PrintWriter, StringWriter}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/10
  * Time: 14:23
  *
  * @author zbs
  * @version 1.0
  */
object Spark_Http_Util {
  val CODE_UTF8 = "UTF-8"
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @Author zbs
    * @Description:http_get
    * @Date: 2019/12/10
    * @Param url:
    * @Param header:
    * @return: java.lang.String
    */
  def get(url: String, header: String = null): String = {
    var response: CloseableHttpResponse = null
    try {
      val client: CloseableHttpClient = HttpClients.createDefault()
      val httpGet = new HttpGet(url)
      //设置header
      if (header != null) {
        val json: JSONObject = JSON.parseObject(header)
        json.keySet()
          .toArray.map(_.toString)
          .foreach(key => httpGet.setHeader(key, json.getString(key)))
      }
      // 发送请求
      response = client.execute(httpGet)

      // 获取返回结果

    }
    catch {
      case ex: Exception => {
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        ex.printStackTrace(pw)
        logger.error(s"http get请求,error:\n${sw.toString}")
      }

    }
    EntityUtils.toString(response.getEntity, CODE_UTF8)
  }

  /**
    * @Author zbs
    * @Description:http_post
    * @Date: 2019/12/10
    * @Param url:
    * @Param params:
    * @Param header:
    * @return: java.lang.String
    */
  def post(url: String, params: String, header: String = null) = {
    var response: CloseableHttpResponse = null
    try {
      val client: CloseableHttpClient = HttpClients.createDefault()
      val httpPost = new HttpPost(url)
      //设置header
      if (header != null) {
        val json: JSONObject = JSON.parseObject(header)
        json.keySet()
          .toArray.map(_.toString)
          .foreach(key => httpPost.setHeader(key, json.getString(key)))
      }
      if (params != null) {
        httpPost.setEntity(new StringEntity(params))
      }
      response = client.execute(httpPost)

    }
    catch {
      case ex: Exception => {
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        ex.printStackTrace(pw)
        logger.error(s"http get请求,error:\n${sw.toString}")
      }
    }
    EntityUtils.toString(response.getEntity, CODE_UTF8)
  }
}
