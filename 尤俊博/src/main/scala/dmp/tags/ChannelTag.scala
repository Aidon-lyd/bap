package dmp.tags

import dmp.traits.TagMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

//渠道标签
object ChannelTag extends TagMaker{
  override def make(row: Row, dic: collection.Map[String, String] = null): Map[String, Double] = {
    //获取渠道信息
    val channelId: String = row.getAs[String]("channelid")

    //计算标签，并返回
    if(StringUtils.isNoneBlank(channelId)){
      Map("CN"+channelId -> 1.0)
    }
    else Map[String, Double]()
  }
}
