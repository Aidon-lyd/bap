package com.cmcc.dmp.label

import java.util

import com.cmcc.dmp.Utils.{DateUtils, ESUtils, HBaseUtils, HbaseScalaUtils, LabelUtils, SparkHelper, UserIdUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


import scala.language.postfixOps


/**
  * requirement  聚合标签 做衰减处理 存入HBase 和 ES
  *
  * @author zhangsl
  * @date 2019/12/7 10:38 
  * @version 1.0
  */
object AdvertisementLabel2 {


  /**
    * 整合标签 写入HBase
    *
    * @param appName
    * @param master
    * @param filePath
    * @param tableName
    * @param columnDescriptor
    * @param day
    */
  def getLabel(appName: String, master: String, filePath: String,
               tableName:String,columnDescriptor:String,
               totalLabeltableName:String,totalLabelcolumnDescriptor:String,
               day:String): Unit = {
    // 获取sparkConf对象
    val conf: SparkConf = SparkHelper.getSparkConf(appName,master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("es.nodes","hadoop01,hadoop02,hadoop03")
//    conf.set("es.port","9200")
//    conf.set("es.index.auto.create","true")
    //conf.set("es.set.netty.runtime.available.processors", "false")
    //conf.set("es.index.auto.create", "true")

    val context: SparkContext = new SparkContext(conf)
    //val sQLContext = new SQLContext(context)
    val spark: SparkSession = SparkHelper.getSpark(conf)

    import spark.implicits._

    // 读取数据文件
    val dataFrame: DataFrame = spark.read.parquet(filePath)
    //println(dataFrame.count())

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
    HBaseUtils.createTable(tableName,columnDescriptor)
    HBaseUtils.addColumnDescriptor(tableName,columnDescriptor)
    HBaseUtils.createTable(totalLabeltableName,totalLabelcolumnDescriptor+DateUtils.getTimeAgo(day))
    HBaseUtils.addColumnDescriptor(totalLabeltableName,totalLabelcolumnDescriptor+day)


    // 用户不为空
    val userIdOne =
      """
        |imei !='' or mac!='' or idfa != '' or openudid!='' or androidid!='' or
        |imeimd5 !='' or macmd5!='' or idfamd5 != '' or openudidmd5!='' or androididmd5!='' or
        |imeisha1 !='' or macsha1!='' or idfasha1 != '' or openudidsha1!='' or androididsha1!=''
      """.stripMargin


    // 格式化 将所有userId放入集合
//    println(dataFrame
//      .filter(userIdOne).count())
    val userall: RDD[(List[String], Row)] = dataFrame
      .filter(userIdOne)
      .rdd
      .map(row => {
        val userIdList: List[String] = UserIdUtils.getAllUserId(row)
        (userIdList, row)
      })
    //userall.foreach(println)
    println(userall.count())

    // 构建点集合
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
    //println(pointRDD.count())

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
    //println(vertices.collect().toBuffer)

    // 得到当前数据聚合结果
    val nowRdd: RDD[(String, List[(String, Double)])] = vertices.join(pointRDD)
      .map {
        case (userId, (commonId, label)) => (commonId, label)
      }
      .reduceByKey {
        case (list1, list2) => (list1 ++ list2)
      }
      .map(x => {
        val list = x._2.groupBy(_._1)
          .mapValues(_.foldLeft(0.0)(_ + _._2)).toList
        (x._1.toString, list)
      })
    // 将今天的数据先存出一份 在hbase dmp_label_test 表中
    nowRdd.foreachPartition(rdd=>{
      val table = HBaseUtils.getTable(tableName)
      if (rdd!=null){
        rdd.foreach(x=>{
          val rowkey = x._1.toString
          val info: String = x._2.map(x=>x._1+":"+x._2).mkString(",")
          HBaseUtils.putData(table,columnDescriptor,rowkey,day,info)
        })
      }
    })
    //nowRdd.foreach(println)

   // println("-------------------------------------------------")

    // 获取昨天的数据 在聚合表 hbase dmp_totalLabel中获取
    val oldRDD: RDD[(String, List[(String, Double)])] =
      HbaseScalaUtils.getYesterdayData(context,totalLabeltableName,totalLabelcolumnDescriptor+DateUtils.getTimeAgo(day),day)
    //oldRDD.foreach(println)

    // 得到今天和昨天数据的聚合结果 权重公式  x*0.8+y
    val resRDD: RDD[(String, List[(String, Double)])] = oldRDD.union(nowRdd)
      .reduceByKey {
        (list1, list2) => (list1 ++ list2)
      }
      .map(x => {
        val list = x._2.groupBy(_._1)
          .mapValues(_.foldLeft(0.0)(_*0.8 + _._2)).toList
        (x._1, list)
      })

    // 转成json格式的 方便存储ES
    val jsonDS: DataFrame = resRDD.mapPartitions(_.map(data => {
      val label: String = data._2.map(x => x._1 + ":" + x._2).mkString(",")
      (data._1, label)
    }))
      .toDF("rowkey", "label")
    jsonDS.show(false)

    // 存储ES  生产上做动态参数
    val IndexName = "dmptest";
    //EsSparkSQL.saveToEs(jsonDS,IndexName+"/"+day)
    jsonDS.toJSON.foreachPartition(rdd=>{
      if (rdd!=null){
        val client = ESUtils.getClient()
        rdd.foreach(x=>{
          ESUtils.putData(client,x,IndexName,"Tags")
          println(x)
        })
       // ESUtils.cleanUp(client)
      }
    })


     // 将聚合结果写入聚合库中
    resRDD.foreachPartition(rdd=>{
        val table = HBaseUtils.getTable(totalLabeltableName)
        if (rdd!=null){
          rdd.foreach(x=>{
            val rowkey = x._1.toString
            val info = x._2.map(x=>x._1+":"+x._2).mkString(",")
            HBaseUtils.putData(table,totalLabelcolumnDescriptor+day,rowkey,day,info)
          })
        }
      })

    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val master = "local[*]"
    val filePath = "outAnli/*.parquet"
    //val filePath = "outAnli/part-00000-fcb9f7fc-ab8c-4682-b630-0508738b0f95-c000.snappy.parquet"
    val tableName = "dmp_label_test"
    val totalLabeltableName = "dmp_totalLabel"
    val day = "20191214"
    val columnDescriptor = "tags"+day
    val totalLabelcolumnDescriptor = "totalTags"
    getLabel(appName,master,filePath,tableName,columnDescriptor,totalLabeltableName,totalLabelcolumnDescriptor,day)
  }
}
