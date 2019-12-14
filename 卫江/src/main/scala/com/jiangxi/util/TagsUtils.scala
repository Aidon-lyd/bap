package com.jiangxi.util
import com.jiangxi.entry.logBean
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsUtils {

  // 过滤条件
  val userIdOne =
    """
      |imei !='' or mac!='' or idfa != '' or openudid!='' or androidid!='' or
      |imeimd5 !='' or macmd5!='' or idfamd5 != '' or openudidmd5!='' or androididmd5!='' or
      |imeisha1 !='' or macsha1!='' or idfasha1 != '' or openudidsha1!='' or androididsha1!=''
    """.stripMargin

  def getAllUserIds(log:logBean):String={
    log match {
      case log if StringUtils.isNoneBlank(log.imei) => "IM: "+(log.imei)
      case log if StringUtils.isNoneBlank(log.mac) => "MC: "+(log.mac)
      case log if StringUtils.isNoneBlank(log.idfa) => "ID: "+(log.idfa)
      case log if StringUtils.isNoneBlank(log.openudid) => "OD: "+(log.openudid)
      case log if StringUtils.isNoneBlank(log.androidid) => "AD: "+(log.androidid)
      case log if StringUtils.isNoneBlank(log.imeimd5) => "IMMD5: "+(log.imeimd5)
      case log if StringUtils.isNoneBlank(log.macmd5) => "MCMD5: "+(log.macmd5)
      case log if StringUtils.isNoneBlank(log.idfamd5) => "IDMD5: "+(log.idfamd5)
      case log if StringUtils.isNoneBlank(log.openudidmd5) => "ODMD5: "+(log.openudidmd5)
      case log if StringUtils.isNoneBlank(log.androididmd5) => "ADMD5: "+(log.androididmd5)
      case log if StringUtils.isNoneBlank(log.imeimd5) => "IMS1: "+(log.imeimd5)
      case log if StringUtils.isNoneBlank(log.macsha1) => "MCS1: "+(log.macsha1)
      case log if StringUtils.isNoneBlank(log.idfasha1) => "IDS1: "+(log.idfasha1)
      case log if StringUtils.isNoneBlank(log.openudidsha1) => "ODS1: "+(log.openudidsha1)
      case log if StringUtils.isNoneBlank(log.androididsha1) => "ADS1: "+(log.androididsha1)
    }
  }
  // 获取当前数据所有的UserId
  def getAllUserId(v:Row):List[String]={
    var list =List[String]()
    if(StringUtils.isNoneBlank(v.getAs[String]("imei"))) list:+= "IM: "+v.getAs[String]("imei")
    if(StringUtils.isNoneBlank(v.getAs[String]("mac")))  list:+= "MC: "+v.getAs[String]("mac")
    if(StringUtils.isNoneBlank(v.getAs[String]("idfa")))  list:+= "ID: "+v.getAs[String]("idfa")
    if(StringUtils.isNoneBlank(v.getAs[String]("openudid"))) list:+=  "OD: "+v.getAs[String]("openudid")
    if(StringUtils.isNoneBlank(v.getAs[String]("androidid"))) list:+=  "ANDROIDI: "+v.getAs[String]("androidid")
    if(StringUtils.isNoneBlank(v.getAs[String]("imeimd5")))  list:+= "IMMD5: "+v.getAs[String]("imeimd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("macmd5")))  list:+= "MCMD5: "+v.getAs[String]("macmd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("idfamd5")))  list:+= "IDMD5: "+v.getAs[String]("idfamd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("openudidmd5")))  list:+= "ODMD5: "+v.getAs[String]("openudidmd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("androididmd5"))) list:+=  "ADMD5: "+v.getAs[String]("androididmd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("imeisha1")))  list:+= "IMS1: "+v.getAs[String]("imeisha1")
    if(StringUtils.isNoneBlank(v.getAs[String]("macsha1")))  list:+= "MCS1: "+v.getAs[String]("macsha1")
    if(StringUtils.isNoneBlank(v.getAs[String]("idfasha1")))  list:+= "IDS1: "+v.getAs[String]("idfasha1")
    if(StringUtils.isNoneBlank(v.getAs[String]("openudidsha1")))  list:+= "ODS1: "+v.getAs[String]("openudidsha1")
    if(StringUtils.isNoneBlank(v.getAs[String]("androididsha1"))) list:+=  "ADS1: "+v.getAs[String]("androididsha1")
    list
  }
}
