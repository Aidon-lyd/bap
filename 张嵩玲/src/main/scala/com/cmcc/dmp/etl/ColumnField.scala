package com.cmcc.dmp.etl

import scala.collection.mutable.ArrayBuffer

/**
  * requirement  DW层 字段
  *
  * @author zhangsl
  * @date 2019/11/26 20:21 
  * @version 1.0
  */
object ColumnField {
  /**
    * 统计各省市数据量分布情况报表
    * @return
    */
  def selectprovDataDistribution:ArrayBuffer[String] = {
    val column =new ArrayBuffer[String]()
    column.append("ct")
    column.append("provincename")
    column.append("cityname")
    column
  }

  def selectoriginalReqTotal:ArrayBuffer[String] = {
    val column =new ArrayBuffer[String]()
    column.append("provincename")
    column.append("cityname")
    column.append("originalReqTotal")
    column
  }

  def selectvalidReqTotal:ArrayBuffer[String] = {
    val column =new ArrayBuffer[String]()
    column.append("provincename")
    column.append("cityname")
    column.append("validReqTotal")
    column
  }

  def selectadvertisingReqTotal:ArrayBuffer[String] = {
    val column =new ArrayBuffer[String]()
    column.append("provincename")
    column.append("cityname")
    column.append("advertisingReqTotal")
    column
  }



  def selectAreaDistribution:ArrayBuffer[String] = {
    val column =new ArrayBuffer[String]()
    column.append("provincename")
    column.append("cityname")
    column.append("originalReqTotal")
    column.append("validReqTotal")
    column.append("advertisingReqTotal")
    column.append("biddingTotal")
    column.append("biddingSuccTotal")
    column.append("biddingSuccRate")
    column.append("showTotal")
    column.append("clickTotal")
    column.append("clickRate")
    column.append("DSPadvertisingExp")
    column.append("SPadvertisingCost")
    column
  }


  def selectReq:ArrayBuffer[String] = {
    val column =new ArrayBuffer[String]()
    column.append("provincename")
    column.append("cityname")
    column.append("ifnull(originalReqTotal,0)")
    column.append("ifnull(validReqTotal,0)")
    column.append("ifnull(advertisingReqTotal,0)")
    column
  }
  def selectBidd:ArrayBuffer[String] = {
    val column =new ArrayBuffer[String]()
    column.append("provincename")
    column.append("cityname")
    column.append("ifnull(biddingTotal,0) as biddingTotal")
    column.append("ifnull(biddingSuccTotal,0) as biddingSuccTotal")
    column
  }

  def selectBidddouble:ArrayBuffer[String] = {
    val column =new ArrayBuffer[String]()
    column.append("provincename")
    column.append("cityname")
    column.append("biddingTotal")
    column.append("biddingSuccTotal")
    column.append("aaa")
    column
  }





}
