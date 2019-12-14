package com.cmcc.dmp.label

import com.cmcc.dmp.Utils.{DmpLOGObjrct, HBaseUtils, LabelUtils, SparkHelper}
import com.cmcc.dmp.label.businessLabel.getBusinessLabelTest

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import util.control.Breaks._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.language.postfixOps


/**
  * requirement  聚合标签写入HBase
  *
  * @author zhangsl
  * @date 2019/12/7 10:38 
  * @version 1.0
  */
object AdvertisementLabel {

  val tableName = "dmp_label"
  val columnDescriptor = "tags"
  val day = "20191209"

  def getLabel(appName: String, master: String, filePath: String): Unit = {
    // 获取sparkConf对象
    val conf: SparkConf = SparkHelper.getSparkConf(appName,master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkHelper.getSpark(conf)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 读取数据文件
    val dataFrame: DataFrame = spark.read.parquet(filePath)
    // 加载字典文件 并广播
    val dictData = context.textFile("log/dick/app_dict.txt")
    val map = dictData
      .filter(_.split("\t",-1).length>=5)
      .map(t=>{
        (t.split("\t",-1)(4),t.split("\t",-1)(1))
      })
      .collectAsMap()
    val dick: Broadcast[collection.Map[String, String]] = context.broadcast(map)

    // 广播停用词库
    val stopWord = context.textFile("log/dick/stopwords.txt")
    val arrWord = stopWord.collect()
    val stopWordArr: Broadcast[Array[String]] = context.broadcast(arrWord)

    // 创建表
    //HBaseUtils.createTable(tableName,columnDescriptor)


    // 用户不为空
    val userIdOne =
      """
        |imei !='' or mac!='' or idfa != '' or openudid!='' or androidid!='' or
        |imeimd5 !='' or macmd5!='' or idfamd5 != '' or openudidmd5!='' or androididmd5!='' or
        |imeisha1 !='' or macsha1!='' or idfasha1 != '' or openudidsha1!='' or androididsha1!=''
      """.stripMargin

    val userIdAndLabel = dataFrame
        .filter(userIdOne)
        .mapPartitions(_.map(row=>{
          // 获取userID
          val userID = LabelUtils.getUserID(row)
          // 获取广告标签
          val advertisementLabel = LabelUtils.getAdvertisementLabel(row)
          // 获取APP名称标签
          val appNameLabel = LabelUtils.getAppNameLabel(row,dick)
          // 获取渠道标签
          val adplatformprovideridLabel = LabelUtils.getAdplatformprovideridLabel(row)
          // 获取设备标签
          val deviceLabel = LabelUtils.getDeviceLabel(row)
          // 获取关键字标签
          val keyWordLabel = LabelUtils.getKeywordLabel(row,stopWordArr)
          // 获取地域标签
          val locationLabel = LabelUtils.getLocationLabel(row)
          //获得商圈标签
          //val businessList = getBusinessLabelTest(row)
          //(userID,advertisementLabel++appNameLabel++adplatformprovideridLabel++deviceLabel++keyWordLabel++locationLabel++businessList)
          (userID,advertisementLabel++appNameLabel++adplatformprovideridLabel++deviceLabel++keyWordLabel++locationLabel)
        }))

//    val filter: RDD[(String, List[(String, Int)])] = userIdAndLabel.rdd.filter(_._1.length>0)
//    val grouped: RDD[(String, Iterable[List[(String, Int)]])] = filter.groupByKey()
//
//    val res: RDD[(String, ListBuffer[(String, Int)])] = grouped.map(every => {
//      val falt: List[(String, Int)] = every._2.toList.flatten
//      val group: Map[String, List[(String, Int)]] = falt.groupBy(_._1)
//      val list: ListBuffer[(String, Int)] = new ListBuffer[(String, Int)]()
//      group.foreach(t => {
//        list.append((t._1, t._2.length))
//      })
//      (every._1, list)
//    })
//    res.foreach(println)


    //val ResLabel: RDD[(String, List[(String, Int)])] =
      userIdAndLabel.rdd.reduceByKey((list1, list2) => (list1 ::: list2))
      .map(x => {
        val list = x._2.groupBy(_._1)
          .mapValues(_.foldLeft(0)(_ + _._2))
          .toList
        (x._1, list)
      })
      .foreach(println)
//      .foreachPartition(rdd=>{
//        val table = HBaseUtils.getTable(tableName)
//        if (rdd!=null){
//          rdd.foreach(x=>{
//            val rowkey = x._1
//            val info = x._2.map(x=>x._1+":"+x._2).mkString(",")
//            HBaseUtils.putData(table,columnDescriptor,rowkey,day,info)
//          })
//        }
//      })

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val master = "local[*]"
    val filePath = "outAnli/*.parquet"
    getLabel(appName,master,filePath)
  }
}
