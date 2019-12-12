package com.cmcc.dmp.label

import com.cmcc.dmp.Utils.{HBaseUtils, LabelUtils, SparkHelper, UserIdUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps


/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/7 10:38 
  * @version 1.0
  */
object AdvertisementLabel2 {


  /**
    * 整合标签 写入HBase
    * @param appName
    * @param master
    * @param filePath
    * @param tableName
    * @param columnDescriptor
    * @param day
    */
  def getLabel(appName: String, master: String, filePath: String,tableName:String,columnDescriptor:String,day:String): Unit = {
    // 获取sparkConf对象
    val conf: SparkConf = SparkHelper.getSparkConf(appName,master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context: SparkContext = new SparkContext(conf)
    //val sQLContext = new SQLContext(context)
    val spark: SparkSession = SparkHelper.getSpark(conf)

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
   // HBaseUtils.createTable(tableName,columnDescriptor)


    // 用户不为空
    val userIdOne =
      """
        |imei !='' or mac!='' or idfa != '' or openudid!='' or androidid!='' or
        |imeimd5 !='' or macmd5!='' or idfamd5 != '' or openudidmd5!='' or androididmd5!='' or
        |imeisha1 !='' or macsha1!='' or idfasha1 != '' or openudidsha1!='' or androididsha1!=''
      """.stripMargin


    val userall: RDD[(List[String], Row)] = dataFrame
      .filter(userIdOne)
      .rdd
      .map(row => {
        val userIdList: List[String] = UserIdUtils.getAllUserId(row)
        (userIdList, row)
      })
    //userall.foreach(println)

    val pointRDD: RDD[(Long, List[(String, Int)])] = userall
      //.mapPartitions(_.map(line=>{
      .flatMap(line => {
      val userIdList = line._1
      val row = line._2
      // 获取广告标签
      val advertisementLabel = LabelUtils.getAdvertisementLabel(row)
      // 获取APP名称标签
      val appNameLabel = LabelUtils.getAppNameLabel(row, dick)
      // 获取渠道标签
      val adplatformprovideridLabel = LabelUtils.getAdplatformprovideridLabel(row)
      // 获取设备标签
      val deviceLabel = LabelUtils.getDeviceLabel(row)
      // 获取关键字标签
      val keyWordLabel = LabelUtils.getKeywordLabel(row, stopWordArr)
      // 获取地域标签
      val locationLabel = LabelUtils.getLocationLabel(row)
      //获得商圈标签
      //val businessList = getBusinessLabelTest(row)
      // 标签集合
      val labelList = advertisementLabel ++ appNameLabel ++ adplatformprovideridLabel ++ deviceLabel ++ keyWordLabel ++ locationLabel
      val uidAndLabelList = userIdList.map((_, 0)) ++ labelList
      userIdList.map(uid => {
        // 如果是第一个元素就携带数据 否则不携带 避免重复累加
        if (userIdList.head.equals(uid)) {
          (uid.hashCode.toLong, uidAndLabelList)
        } else{
          (uid.hashCode.toLong, List.empty)
        }
      })
    })
    //pointRDD.foreach(println)

    // 构建边集合 RDD[Edge[ED]]  Edge(21416623,24061958,0)
    val EdgeRDD: RDD[Edge[Int]] = userall
      .map(_._1)
      .flatMap(arr => {
        val headUidLong: Long = arr.head.hashCode.toLong
        arr.map(uid => Edge(headUidLong, uid.hashCode.toLong, 0))
      })
    //EdgeRDD.foreach(println)


    // 构建图对象
    val graph: Graph[List[(String, Int)], Int] = Graph(pointRDD,EdgeRDD)

    // 调用API 取到图的顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    println(vertices.collect().toBuffer)

    vertices.join(pointRDD)
        .map{
          case (userId,(commonId,label)) => (commonId,label)
        }
      .reduceByKey{
        case (list1,list2) => (list1++list2)
      }
      .map(x=>{
        val list = x._2.groupBy(_._1)
          .mapValues(_.foldLeft(0)(_ + _._2)).toList
        (x._1,list)
      })
      //.foreach(println)
//      .foreachPartition(rdd=>{
//        val table = HBaseUtils.getTable(tableName)
//        if (rdd!=null){
//          rdd.foreach(x=>{
//            val rowkey = x._1.toString
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
    val tableName = "dmp_label_graph"
    val columnDescriptor = "tags"
    val day = "20191211"
    getLabel(appName,master,filePath,tableName,columnDescriptor,day)
  }
}
