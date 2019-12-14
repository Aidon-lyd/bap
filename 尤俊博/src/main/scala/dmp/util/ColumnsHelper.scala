package dmp.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object ColumnsHelper {

  /**
    * 不用textFIle读取而是用schema.csv读取时传入的元数据
    * @return
    */
  def getStructType(): StructType ={
    val scheam= StructType(
      Array(
        StructField("sessionid", StringType, true),
        StructField("advertisersid", LongType, true),
        StructField("adorderid", LongType, true) ,
        StructField("adcreativeid", LongType, true),
        StructField("adplatformproviderid", LongType, true),
        StructField("sdkversion", StringType, true),
        StructField("adplatformkey", StringType, true),
        StructField("putinmodeltype", LongType, true) ,
        StructField("requestmode", LongType, true),
        StructField("adprice", DoubleType, true),
        StructField("adppprice", DoubleType, true),
        StructField("requestdate", StringType, true),
        StructField("ip", StringType, true),
        StructField("appid", StringType, true) ,
        StructField("appname", StringType, true),
        StructField("uuid", StringType, true),
        StructField("device", StringType, true),
        StructField("client", LongType, true),
        StructField("osversion", StringType, true),
        StructField("density", StringType, true) ,
        StructField("pw", LongType, true),
        StructField("ph", LongType, true),
        StructField("long", StringType, true),
        StructField("lat", StringType, true),
        StructField("provincename", StringType, true),
        StructField("cityname", StringType, true),
        StructField("ispid", LongType, true),
        StructField("ispname", StringType, true),
        StructField("networkmannerid", LongType, true),
        StructField("networkmannername", StringType, true),
        StructField("iseffective", LongType, true),
        StructField("isbilling", LongType, true),
        StructField("adspacetype", LongType, true),
        StructField("adspacetypename", StringType, true),
        StructField("devicetype", LongType, true),
        StructField("processnode", LongType, true),
        StructField("apptype", LongType, true),
        StructField("district", StringType, true),
        StructField("paymode", LongType, true),
        StructField("isbid", LongType, true),
        StructField("bidprice", DoubleType, true),
        StructField("winprice", DoubleType, true),
        StructField("iswin", LongType, true),
        StructField("cur", StringType, true),
        StructField("rate", DoubleType, true),
        StructField("cnywinprice", DoubleType, true),
        StructField("imei", StringType, true),
        StructField("mac", StringType, true),
        StructField("idfa", StringType, true),
        StructField("openudid", StringType, true),
        StructField("androidid", StringType, true),
        StructField("rtbprovince", StringType, true),
        StructField("rtbcity", StringType, true),
        StructField("rtbdistrict", StringType, true),
        StructField("rtbstreet", StringType, true),
        StructField("storeurl", StringType, true),
        StructField("realip", StringType, true),
        StructField("isqualityapp", LongType, true),
        StructField("bidfloor", DoubleType, true),
        StructField("aw", LongType, true),
        StructField("ah", LongType, true),
        StructField("imeimd5", StringType, true),
        StructField("macmd5", StringType, true),
        StructField("idfamd5", StringType, true),
        StructField("openudidmd5", StringType, true),
        StructField("androididmd5", StringType, true),
        StructField("imeisha1", StringType, true),
        StructField("macsha1", StringType, true),
        StructField("idfasha1", StringType, true),
        StructField("openudidsha1", StringType, true),
        StructField("androididsha1", StringType, true),
        StructField("uuidunknow", StringType, true),
        StructField("userid", StringType, true),
        StructField("iptype", LongType, true),
        StructField("initbidprice", DoubleType, true),
        StructField("adpayment", DoubleType, true),
        StructField("agentrate", DoubleType, true),
        StructField("lomarkrate", DoubleType, true),
        StructField("adxrate", DoubleType, true),
        StructField("title", StringType, true),
        StructField("keywords", StringType, true),
        StructField("tagid", StringType, true),
        StructField("callbackdate", StringType, true),
        StructField("channelid", StringType, true),
        StructField("mediatype", LongType, true)
      ))
    scheam
  }

  //地域分布列--->省
  def getLocationProColumns():ArrayBuffer[Column] = {
    val columns = new ArrayBuffer[Column]()
    columns.+=(expr("SUM(IF(requestmode = 1 and processnode >= 1,1,0))").as("rawRequestNum"))
    columns.+=(expr("SUM(IF(requestmode = 1 and processnode >= 2,1,0))").as("validRequestNum"))
    columns.+=(expr("SUM(IF(requestmode = 1 and processnode = 3,1,0))").as("adRequestNum"))
    columns.+=(expr("SUM(IF(iseffective = 1 and isbilling = 1 and isbid = 1,1,0))").as("participateInBindNum"))
    columns.+=(expr("SUM(IF(iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0,1,0))").as("bindAndWinNum"))
    columns.+=(expr("SUM(IF(requestmode = 2 and iseffective = 1,1,0))").as("explodeNum"))
    columns.+=(expr("SUM(IF(requestmode = 3 and iseffective = 1,1,0))").as("clickNum"))
    columns.+=(expr("SUM(IF(iseffective = 1 and isbilling = 1 and iswin = 1,adpayment/1000,0))").as("adCost"))
    columns.+=(expr("SUM(IF(iseffective = 1 and isbilling = 1 and iswin = 1,winprice/1000,0))").as("adConsume"))

    columns
  }

  //地域分布最终
  def getLocationFinalColumns(colName: String *) = {
    val columns = new ArrayBuffer[String]()

    for(colName <- colName){
      columns.+=(colName)
    }

    columns.+=("rawRequestNum")
    columns.+=("validRequestNum")
    columns.+=("adRequestNum")
    columns.+=("participateInBindNum")
    columns.+=("bindAndWinNum")
    columns.+=("bindAndWinNum/participateInBindNum as bindSuccessRate")
    //May Need to use following Expression
    //"if(bindAndWinNum/participateInBindNum is null,0.0,bindAndWinNum/participateInBindNum )as bindSuccessRate",
    columns.+=("explodeNum")
    columns.+=("clickNum")
    columns.+=("clickNum/explodeNum as clickRate")
    columns.+=("adCost")
    columns.+=("adConsume")
    columns
  }
}
