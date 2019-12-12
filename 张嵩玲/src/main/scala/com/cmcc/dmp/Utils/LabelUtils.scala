package com.cmcc.dmp.Utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/11 11:45 
  * @version 1.0
  */
object LabelUtils {
  /**
    * 广告位类型
    * @param args  每行数据
    * @return list集合
    */
  def getAdvertisementLabel(args: Any*): List[(String,Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val adspacetype = row.getAs[Int]("adspacetype")
    val adspacetypename = row.getAs[String]("adspacetypename")
    adspacetype match {
      case x if x < 10 =>  list:+=(("LC0"+x,1))
      case x if x > 9 => list:+=(("LC"+x , 1))
    }
    if (StringUtils.isNotBlank(adspacetypename)){
      list:+=(("LN"+adspacetypename,1))
    }
    list
  }

  /**
    * APP名称标签
    * @param args  每行数据
    * @return list集合
    */
  def getAppNameLabel(args: Any*): List[(String,Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val dirMap = args(1).asInstanceOf[Broadcast[collection.Map[String,String]]]
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if (StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else{
      list:+=("APP"+dirMap.value.getOrElse(appid,appid),1)
    }
    list
  }

  /**
    * 渠道标签
    * @param args  每行数据
    * @return list集合
    */
  def getAdplatformprovideridLabel(args: Any*): List[(String,Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+adplatformproviderid , 1)
    list
  }

  /**
    * 设备标签
    * @param args  每行数据
    * @return list集合
    */
  def getDeviceLabel(args: Any*): List[(String,Int)] = {

    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val client = row.getAs[Int]("client")
    val networkmannername = row.getAs[String]("networkmannername")
    val ispname = row.getAs[String]("ispname")
    client match {
      case x if x ==1 => list:+=("Android D00010001"->1)
      case x if x ==2 => list:+=("IOS D00010002"->1)
      case x if x ==3 => list:+=("WinPhone D00010003"->1)
      case _ => list:+=("其他 D00010004"->1)
    }
    networkmannername match {
      case x if x == "Wifi" => list:+=("WIFI D00020001"->1)
      case x if x == "4G" => list:+=("4G D00020002"->1)
      case x if x == "3G" => list:+=("3G D00020003"->1)
      case x if x == "2G" => list:+=("2G D00020004"->1)
      case _ => list:+=("D00020005"->1)
    }
    ispname match {
      case x if x == "移动" => list:+=("移 动 D00030001"->1)
      case x if x == "联通" => list:+=("联 通 D00030002"->1)
      case x if x == "电信" => list:+=("电 信 D00030003"->1)
      case _ => list:+=("D00030004"->1)
    }
    list
  }

  /**
    * 关键字标签
    * @param args  每行数据
    * @return list集合
    */
  def getKeywordLabel(args: Any*): List[(String,Int)] = {
    var list = List[(String,Int)]()
    val array = new ArrayBuffer[String]()
    val row: Row = args(0).asInstanceOf[Row]
    val kw = args(1).asInstanceOf[Broadcast[Array[String]]]
    val keywords = row.getAs[String]("keywords")
    keywords.split("\\|").filter(word=>{
      word.length>=3 && word.length<=8 && !kw.value.contains(word)
    }).foreach(f=>{
      list:+=("K"+f,1)
    })
    list
  }

  /**
    * 地域标签
    * @param args  每行数据
    * @return list集合
    */
  def getLocationLabel(args: Any*): List[(String,Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(provincename)){

      list:+=("ZP"+provincename , 1)
    }
    if (StringUtils.isNotBlank(cityname)){

      list:+=("ZC"+cityname,1)
    }
    list
  }

  /**
    * 获取 userId
    * @param args
    * @return
    */
  def getUserID(args: Any*): String = {
    var userid = ""
    val row: Row = args(0).asInstanceOf[Row]
    val userIdField = "imei,mac,idfa,openudid,androidid,imeimd5,macmd5,idfamd5,openudidmd5,androididmd5,imeisha1,macsha1,idfasha1,openudidsha1,androididsha1"
    val strings: Array[String] = userIdField.split(",")
    breakable {
      for (user <- strings) {
        val res = row.getAs[String](user)
        //println(res)
        if (res.length>0) {
          userid = res
          // println("++++++++++++++"+userid)
          break()
        }
      }
    }
    userid
  }
}
