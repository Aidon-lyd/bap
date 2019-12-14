package com.cmcc.dmp.Utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


/**
  * requirement  获取所有userId
  *
  * @author zhangsl
  * @date 2019/12/11 10:31 
  * @version 1.0
  */

object UserIdUtils {
  def getAllUserId(row:Row):List[String]={
    var list = List[String]()
    if (StringUtils.isNotBlank(row.getAs[String]("imei"))) {
      list:+=row.getAs[String]("imei")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("mac"))) {
      list:+=row.getAs[String]("mac")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("idfa"))) {
      list:+=row.getAs[String]("idfa")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("openudid"))) {
      list:+=row.getAs[String]("openudid")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("androidid"))) {
      list:+=row.getAs[String]("androidid")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5"))) {
      list:+=row.getAs[String]("imeimd5")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5"))) {
      list:+=row.getAs[String]("macmd5")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5"))) {
      list:+=row.getAs[String]("idfamd5")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5"))) {
      list:+=row.getAs[String]("openudidmd5")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5"))) {
      list:+=row.getAs[String]("androididmd5")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1"))) {
      list:+=row.getAs[String]("imeisha1")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1"))) {
      list:+=row.getAs[String]("macsha1")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1"))) {
      list:+=row.getAs[String]("idfasha1")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1"))) {
      list:+=row.getAs[String]("openudidsha1")
    }
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1"))) {
      list:+=row.getAs[String]("androididsha1")
    }
    list
  }
}
