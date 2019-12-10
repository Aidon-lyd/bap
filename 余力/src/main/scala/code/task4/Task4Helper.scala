package code.task4

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

import scala.collection.mutable.ArrayBuffer

object Task4Helper {

  /**
    * Get Column Format One Likes
    *
    * @return
    */
  def getColumnFormatOne() = {
    val columns = new ArrayBuffer[String]()
    //The following column is not sure now
    columns.+=("uuid")

    columns.+=("concat('LC',if(adspacetype < 10,concat('0',adspacetype),adspacetype)) as adSpaceTypeTagLC")
    columns.+=("concat('LN',adspacetypename) as adSpaceTypeTagLN")
    //May need to be Optimized By using appDict
    columns.+=("concat('APP',appname) as appNameTagAPP")
    columns.+=("concat('CN',adplatformproviderid) as channelTagCN")
    //The following three columns can be Optimized By using UDF
    columns.+=("concat('D0001000',client) as deviceTagOS")
    columns.+=("concat('D0002000',networkmannerid) as deviceTagNetWorkingMode")
    columns.+=("concat('D0003000',ispid) as deviceTagEquipmentOperatorMode")
    columns.+=("getKeywordsTag(keywords) as keywordsTagK")
    columns.+=("concat('ZP',provincename) as areaTagZP")
    columns.+=("concat('ZC',cityname) as areaTagZC")
    columns.+=("if(isValidLongAndLat(longitude,lat),getTradeMarkTags(longitude,lat),'') as tradeMarkTag")

    columns
  }

  /**
    * Get Column Format Two Likes
    */
  def getColumnFormatTwo() = {
    val columns = new ArrayBuffer[Column]()

    columns.+=(expr("mergeCommonTag(adSpaceTypeTagLC)").as("mergedAdSpaceTypeTagLC"))
    columns.+=(expr("mergeCommonTag(adSpaceTypeTagLN)").as("mergedAdSpaceTypeTagLN"))
    columns.+=(expr("mergeCommonTag(appNameTagAPP)").as("mergedAppNameTagAPP"))
    columns.+=(expr("mergeCommonTag(channelTagCN)").as("mergedChannelTagCN"))
    columns.+=(expr("mergeCommonTag(deviceTagOS)").as("mergedDeviceTagOS"))
    columns.+=(expr("mergeCommonTag(deviceTagNetWorkingMode)").as("mergedDeviceTagNetWorkingMode"))
    columns.+=(expr("mergeCommonTag(deviceTagEquipmentOperatorMode)").as("mergedDeviceTagEquipmentOperatorMode"))
    columns.+=(expr("mergeSpecialTag(keywordsTagK)").as("mergedKeywordsTagK"))
    columns.+=(expr("mergeCommonTag(areaTagZP)").as("mergedAreaTagZP"))
    columns.+=(expr("mergeCommonTag(areaTagZC)").as("mergedAreaTagZC"))
    columns.+=(expr("mergeSpecialTag(tradeMarkTag)").as("mergedTradeMarkTag"))

    columns
  }
}
