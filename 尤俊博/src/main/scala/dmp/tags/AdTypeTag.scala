package dmp.tags

import dmp.traits.TagMaker
import org.apache.spark.sql.Row

//广告位类型标签
object AdTypeTag extends TagMaker{
  override def make(row: Row, dic: collection.Map[String, String] = null): Map[String, Double] = {
    val adType: Long = row.getAs[Long]("adspacetype")

    //计算并返回标签
    adType match {
      case 1 => Map("LC01" -> 1)
      case 2 => Map("LC02" -> 2)
      case 3 => Map("LC03" -> 3)
      case _ => Map[String, Double]()
    }
  }
}
