package ADS_Tag

import Spark_Utils.SparkSession_Utils
import ods_release.ODS_Relase
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/7
  * Time: 16:59
  *
  * @author zbs
  * @version 1.0
  */
object Ad_Tag extends java.io.Serializable{
  /**
    * @Author zbs
    * @Description:构建数据标签
    * @Date: 2019/12/7
    * @Param spark:
    * @return: void
    */
  def AD_Tag_Dispose(prelin:Seq[Any]) = {
    var list = List[(String,Int)]()
    var adspacetype: String = prelin(32).toString
    if (adspacetype.length <= 10) {
      adspacetype += "0" * (10 - adspacetype.length)
    }
    val adspacetypename: String = prelin(33).toString
    //广告位类型打标签
    list:+=(adspacetype,1)
    //广告位类型名称加标签
    list:+=(adspacetypename,1)
    list
  }


}
