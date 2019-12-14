package dmp.tags

import dmp.traits.TagMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

//设备标签
object DeviceTag extends TagMaker{
  override def make(row: Row, dic: collection.Map[String, String]): Map[String, Double] = {
    //获取标签信息
    val clientName: String = row.getAs[Long]("client").toString
    val networkName: String = row.getAs[String]("networkmannername")
    val ispName: String = row.getAs[String]("ispname")

    //计算并返回标签
    val clientId: String = dic.getOrElse(clientName, "D00010004")
    val networdId: String = dic.getOrElse(networkName, "D00020005")
    val ispId: String = dic.getOrElse(ispName, "D00030004")

    val clientTag = if(StringUtils.isNotBlank(clientId))
      Map(clientId -> 1.0)
    else
      Map[String, Double]()

    val networkTag = if(StringUtils.isNotBlank(networdId))
      Map(networdId -> 1.0)
    else
      Map[String, Double]()

    val ispTag = if(StringUtils.isNotBlank(ispId))
      Map(ispId -> 1.0)
    else
      Map[String, Double]()

    clientTag ++ networkTag ++ ispTag
  }
}
