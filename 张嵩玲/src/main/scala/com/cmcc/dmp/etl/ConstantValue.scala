package com.cmcc.dmp.etl

import org.apache.spark.storage.StorageLevel

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/6 16:57 
  * @version 1.0
  */
object ConstantValue {

  // 缓存级别
  val STORAGELEVEL = StorageLevel.MEMORY_AND_DISK


  // 常量字段
  val PROVINCENAME = "provincename" //设备所在省
  val CITYNAME = "cityname" // 设备所在市
  val SESSIONID = "sessionid" // 会话标识
  val RTBPROVINCE = "rtbprovince" // 省
  val RTBCITY = "rtbcity" //市
  val REQUESTMODE = "requestmode" // 数据请求方式（1:请求、2:展示、3:点击）
  val PROCESSNODE = "processnode" // 流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
  val ISEFFECTIVE = "iseffective"
  val ISBILLING = "isbilling"
  val ISWIN = "iswin"
  val ADORDEERID = "adorderid"

  // 统计各省市数据量分布情况 字段别名
  val CT = "CT" // 数据量
  val ORIGINALREQTOTAL = "originalReqTotal" // 原始请求数
  val VALIDREQTOTAL = "validReqTotal" // 有效请求数
  val ADVERTISINGREQTOTAL = "advertisingReqTotal" // 广告请求数
  val BIDDINGTOTAL = "biddingTotal" // 参与竞价数
  val BIDDINGSUCCTOTAL = "biddingSuccTotal" // 竞价成功数
  val SHOWTOTAL = "showTotal" // 展示数
  val CLICKTOTAL = "clickTotal" // 点击数
  val DSPADVERTISINGEXP = "DSPadvertisingExp" // DSP广告消费
  val SPADVERTISINGCost = "SPadvertisingCost" // DSP广告成本
  val CLICKRATE = "clickRate" // 点击率
  val BIDDINGSUCCRATE = "biddingSuccRate" // 竞价成功率
}
