package dmp.tags

import dmp.traits.TagMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


//APP名称标签
object AppNameTag extends TagMaker{
  override def make(row: Row, dic: collection.Map[String, String]): Map[String, Double] = {
    //获取Appid
    val appId: String = row.getAs[String]("appid")
    val appName: String = dic.getOrElse(appId,"")

    //计算并返回标签
    if(StringUtils.isNoneBlank(appName)){
      Map("APP"+appName -> 1.0)
    }
    else Map[String, Double]()
  }
}
