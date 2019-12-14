package dmp.tags

import dmp.traits.TagMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

//地域标签
object LocTag extends TagMaker{
  override def make(row: Row, dic: collection.Map[String, String] = null): Map[String, Double] = {
    //获取标签信息
    val provincename: String = row.getAs[String]("provincename")
    val cityname: String = row.getAs[String]("cityname")

    //计算并返回标签信息
    val provinceTag = if(StringUtils.isNotBlank(provincename))
      Map("ZP"+provincename -> 1.0)
    else
      Map[String, Double]()

    val cityTag = if(StringUtils.isNotBlank(cityname))
      Map("ZC"+cityname -> 1.0)
    else
      Map[String, Double]()

    provinceTag ++ cityTag
  }
}
