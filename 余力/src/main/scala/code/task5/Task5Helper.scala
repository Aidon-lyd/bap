package code.task5

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

import scala.collection.mutable.ArrayBuffer

object Task5Helper {

  /**
    * Get Column Format One Likes
    */
  def getColumnFormatOne()={
    val columns = new ArrayBuffer[Column]()

    columns.+=(expr("ROW_NUMBER() OVER(ORDER BY imei,mac,idfa,openudid,androidid,imeimd5,macmd5,idfamd5,openudidmd5,androididmd5,imeisha1,macsha1,idfasha1,openudidsha1,androididsha1) as rowNumber"))
    columns.+=(expr("imei"))
    columns.+=(expr("mac"))
    columns.+=(expr("idfa"))
    columns.+=(expr("openudid"))
    columns.+=(expr("androidid"))
    columns.+=(expr("imeimd5"))
    columns.+=(expr("macmd5"))
    columns.+=(expr("idfamd5"))
    columns.+=(expr("openudidmd5"))
    columns.+=(expr("androididmd5"))
    columns.+=(expr("imeisha1"))
    columns.+=(expr("macsha1"))
    columns.+=(expr("idfasha1"))
    columns.+=(expr("openudidsha1"))
    columns.+=(expr("androididsha1"))
    columns.+=(expr("adSpaceTypeTagLC"))
    columns.+=(expr("adSpaceTypeTagLN"))
    columns.+=(expr("appNameTagAPP"))
    columns.+=(expr("channelTagCN"))
    columns.+=(expr("deviceTagOS"))
    columns.+=(expr("deviceTagNetWorkingMode"))
    columns.+=(expr("deviceTagEquipmentOperatorMode"))
    columns.+=(expr("keywordsTagK"))
    columns.+=(expr("areaTagZP"))
    columns.+=(expr("areaTagZC"))
    columns.+=(expr("tradeMarkTag"))

    columns
  }

  /**
    * Get Column Format Two Likes
    */
  def getColumnFormatTwo()={
    val columns = new ArrayBuffer[Column]()

    columns.+=(expr("rowNumber"))
    columns.+=(expr("ROW_NUMBER() OVER(PARTITION BY imei,mac,idfa,openudid,androidid,imeimd5,macmd5,idfamd5,openudidmd5,androididmd5,imeisha1,macsha1,idfasha1,openudidsha1,androididsha1 ORDER BY imei,mac,idfa,openudid,androidid,imeimd5,macmd5,idfamd5,openudidmd5,androididmd5,imeisha1,macsha1,idfasha1,openudidsha1,androididsha1) as idMappingSourceNumber"))
    columns.+=(expr("explode(split(concat_ws(',',if(imei=='',null,imei),if(mac=='',null,mac),if(idfa=='',null,idfa),if(openudid=='',null,openudid),if(androidid=='',null,androidid), if(imeimd5=='',null,imeimd5),if(macmd5=='',null,macmd5),if(idfamd5=='',null,idfamd5),if(openudidmd5=='',null,openudidmd5),if(androididmd5=='',null,androididmd5),if(imeisha1=='',null,imeisha1),if(macsha1=='',null,macsha1),if(idfasha1=='',null,idfasha1),if(openudidsha1=='',null,openudidsha1),if(androididsha1=='',null,androididsha1)),',')) as idMappingSourceID"))
    columns.+=(expr("adSpaceTypeTagLC"))
    columns.+=(expr("adSpaceTypeTagLN"))
    columns.+=(expr("appNameTagAPP"))
    columns.+=(expr("channelTagCN"))
    columns.+=(expr("deviceTagOS"))
    columns.+=(expr("deviceTagNetWorkingMode"))
    columns.+=(expr("deviceTagEquipmentOperatorMode"))
    columns.+=(expr("keywordsTagK"))
    columns.+=(expr("areaTagZP"))
    columns.+=(expr("areaTagZC"))
    columns.+=(expr("tradeMarkTag"))

    columns
  }

  /**
    * Get Column Format Three Likes
    */
  def getColumnFormatThree()={
    //YL:ToBeOptimized May be Optimized By using String Not Column
    val columns = new ArrayBuffer[Column]()

    columns.+=(expr("imei"))
    columns.+=(expr("mac"))
    columns.+=(expr("idfa"))
    columns.+=(expr("openudid"))
    columns.+=(expr("androidid"))
    columns.+=(expr("imeimd5"))
    columns.+=(expr("macmd5"))
    columns.+=(expr("idfamd5"))
    columns.+=(expr("openudidmd5"))
    columns.+=(expr("androididmd5"))
    columns.+=(expr("imeisha1"))
    columns.+=(expr("macsha1"))
    columns.+=(expr("idfasha1"))
    columns.+=(expr("openudidsha1"))
    columns.+=(expr("androididsha1"))

    columns
  }
}
