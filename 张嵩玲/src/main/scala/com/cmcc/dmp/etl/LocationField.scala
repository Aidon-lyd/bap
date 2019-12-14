package com.cmcc.dmp.etl


import com.cmcc.dmp.enums.FlowNodeEnum
import com.cmcc.dmp.enums.RequestEnum
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.storage.StorageLevel

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/6 21:09 
  * @version 1.0
  */
object LocationField {

  def requestUtils(reqMode:Int,prcMode:Int):List[Double]={
    if(reqMode ==1 && prcMode == 1){
      List[Double](1,0,0)
    }else if(reqMode ==1 && prcMode == 2){
      List[Double](1,1,0)
    }else if(reqMode == 1 && prcMode == 3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

  /**
    * 原始请求数
    * @param dataFrame
    * @return
    */
  def getOriginalReqTotal(dataFrame: DataFrame):DataFrame = {
    val originalCondition = (col(s"${ConstantValue.REQUESTMODE}")===lit(RequestEnum.Request.getCode)
      and col(s"${ConstantValue.PROCESSNODE}")>=lit(FlowNodeEnum.TotalRequest.getCode))
    val originalReqTotal: DataFrame = dataFrame
      .where(originalCondition)
      .groupBy(s"${ConstantValue.PROVINCENAME}",s"${ConstantValue.CITYNAME}")
      .agg(count(s"${ConstantValue.SESSIONID}").alias(ConstantValue.ORIGINALREQTOTAL))
      .persist(StorageLevel.MEMORY_AND_DISK)
    return originalReqTotal
  }

  /**
    * 有效请求数
    * @param dataFrame
    * @return
    */
  def getValidReqTotal(dataFrame: DataFrame):DataFrame = {
    val validCondition = (col(s"${ConstantValue.REQUESTMODE}")===lit(RequestEnum.Request.getCode)
      and col(s"${ConstantValue.PROCESSNODE}")>=lit(FlowNodeEnum.Valid.getCode))
    val validReqTotal: DataFrame = dataFrame
      .where(validCondition)
      .groupBy(s"${ConstantValue.PROVINCENAME}",s"${ConstantValue.CITYNAME}")
      .agg(count(s"${ConstantValue.SESSIONID}").alias(ConstantValue.VALIDREQTOTAL))
      .persist(StorageLevel.MEMORY_AND_DISK)
    return validReqTotal
  }

  /**
    * 广告请求数
    * @param dataFrame
    * @return
    */
  def getAdvertisingReqTotal(dataFrame: DataFrame):DataFrame = {
    val advertisingCondition = (col(s"${ConstantValue.REQUESTMODE}")===lit(RequestEnum.Request.getCode)
      and col(s"${ConstantValue.PROCESSNODE}")>=lit(FlowNodeEnum.Advertising.getCode))
    val advertisingReqTotal: DataFrame = dataFrame
      .where(advertisingCondition)
      .groupBy(s"${ConstantValue.PROVINCENAME}",s"${ConstantValue.CITYNAME}")
      .agg(count(s"${ConstantValue.SESSIONID}").alias(ConstantValue.ADVERTISINGREQTOTAL))
      .persist(StorageLevel.MEMORY_AND_DISK)
    return advertisingReqTotal
  }

  /**
    * 参与竞价数
    * @param dataFrame
    * @return
    */
  def getBiddingTotal(dataFrame: DataFrame):DataFrame = {
    val biddingCondition = (col(s"${ConstantValue.ISEFFECTIVE}") === "1"
          and col(s"${ConstantValue.ISBILLING}") === "1"
          and col(s"${ConstantValue.ISWIN}") === "1"
          )
    val biddingTotal: DataFrame = dataFrame
      .where(biddingCondition)
      .groupBy(s"${ConstantValue.PROVINCENAME}",s"${ConstantValue.CITYNAME}")
      .agg(count(s"${ConstantValue.SESSIONID}").alias(ConstantValue.BIDDINGTOTAL))
      .persist(StorageLevel.MEMORY_AND_DISK)
    return biddingTotal
  }

  /**
    * 竞价成功数
    * @param dataFrame
    * @return
    */
  def getBiddingSuccTotal(dataFrame: DataFrame):DataFrame = {
        val biddingSuccCondition = (col(s"${ConstantValue.ISEFFECTIVE}") === "1"
          and col(s"${ConstantValue.ISBILLING}") === "1"
          and col(s"${ConstantValue.ISWIN}") === "1"
          and col(s"${ConstantValue.ADORDEERID}")  =!= 0
          )
    val biddingSuccTotal: DataFrame = dataFrame
      .where(biddingSuccCondition)
      .groupBy(s"${ConstantValue.PROVINCENAME}", s"${ConstantValue.CITYNAME}")
      .agg(count(s"${ConstantValue.SESSIONID}").as(ConstantValue.BIDDINGSUCCTOTAL))
    return biddingSuccTotal
  }

}
