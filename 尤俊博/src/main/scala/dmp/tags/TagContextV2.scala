package dmp.tags

import dmp.traits.Processor
import dmp.util.{ConfigHelper, HbaseUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark._


//标签汇总
object TagContextV2 extends Processor{
  override def processor(spark: SparkSession): Unit = {

    //定义参数
    val sourcePath: String = ConfigHelper.adFormatData
    val appDicFilePath: String = ConfigHelper.appNameDic
    val devicePath: String = ConfigHelper.deviceDic
    val stopWordPath: String = ConfigHelper.stopwordDic



    //读数据
    val data: DataFrame = spark.read.parquet(sourcePath)

    //读app信息(文件)，转换为广播变量(优化)
    val appDicMap: collection.Map[String, String] = spark.sparkContext.textFile(appDicFilePath)
      .filter(line => line.split("\t").length >= 5)
      .map(line => {
        val arr: Array[String] = line.split("\t")
        var appid = ""
        if(arr(3).startsWith("A"))
          appid = arr(4)
        else
          appid = arr(3)
        val appName = arr(1)
        (appid, appName)
      }).collectAsMap()
    val appdicBC: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(appDicMap)

    //读设备字典信息(文件)，转换为广播变量(优化)
    val deviceMap: collection.Map[String, String] = spark.sparkContext.textFile(devicePath)
      .map(line => {
        val arr: Array[String] = line.split("##")
        (arr(0), arr(1))
      }).collectAsMap()
    val deviceBC: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(deviceMap)

    //关键词信息(文件)，转换为广播变量(优化)
    val keywordMap: collection.Map[String, String] = spark.sparkContext.textFile(stopWordPath)
      .map(line => {
        (line, "0")
      }).collectAsMap()
    val stopWordBC: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(keywordMap)

    val baseRDD: RDD[(List[String], Row)] = data.filter(ConfigHelper.filterFields)
      .rdd
      .map(row => {
        val userId: List[String] = getUserIDs(row)
        (userId, row)
      })

    //处理数据，生成标签，并构建点的集合
    val verties: RDD[(Long, List[(String, Double)])] = baseRDD.flatMap(
      tp => {
        val row: Row = tp._2
        //ad channel
        val adTag: Map[String, Double] = AdTypeTag.make(row)
        val channelTag: Map[String, Double] = ChannelTag.make(row)

        //App Device
        val appNameTag: Map[String, Double] = AppNameTag.make(row, appdicBC.value)
        val deviceTag: Map[String, Double] = DeviceTag.make(row, deviceBC.value)

        //key loc
        val keyWordsTags: Map[String, Double] = KeywordsTag.make(row, stopWordBC.value)
        val locTag: Map[String, Double] = LocTag.make(row)

        //商圈标签
        val tradeAreaTagsTmp: Map[String, Double] = TradingAreaTag.make(row)

        //tradeAreaTagsTmp.foreach(println)
        val tradeAreaTags: Map[String, Double] = Map[String, Double]()
        for ((key, value) <- tradeAreaTagsTmp) {
          if (!"SQ无".equals(key)) {
            // println((key -> value).toString())
            tradeAreaTags.+(key -> value)
          }
        }

        //结合所有标签
        val tags: Map[String, Double] = adTag ++ channelTag ++ appNameTag ++ deviceTag ++ keyWordsTags ++ locTag ++ tradeAreaTags

        //点集合的构建
        val VD: List[(String, Double)] = tp._1.map((_, 0.0)) ++ tags.toList

        tp._1.map(uid => {
          if (tp._1.head.equals(uid)) {
            (uid.hashCode.toLong, VD)
          } else {
            (uid.hashCode.toLong, List.empty)
          }
        })
      }
    )

    //verties.take(50).foreach(println)

    //构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uid => Edge(tp._1.head.hashCode, uid.hashCode.toLong, 0))
    })

    //edges.take(10).foreach(println)

    //构建图
    val graph: Graph[List[(String, Double)], Int] = Graph(verties, edges)

    //取出顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    val logTags: RDD[(VertexId, List[(String, Double)])] = vertices.join(verties).map {
      case (uid, (comId, tagsAndUid)) => {
        (comId, tagsAndUid)
      }
    }
      .reduceByKey(
        (list1, list2) => (list1 ++ list2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum).toList)
       //  .take(20).foreach(println)

    logTags.take(10).foreach(println)

    //想要做标签的衰减需要拿取前一天的数据与今天的数据union，然后进行groupBy，然后对标签进行操作

    //存储到Hbase，采用批处理的方式
    // create 'TagsF', 'Day01'
//    val mapTags: collection.Map[VertexId, List[(String, Double)]] = logTags.collectAsMap()
//    val hashMap = new java.util.HashMap[String, String]()
//    for((key,value)<-mapTags){
//      hashMap.put(key.toString,value.toString())
//    }
//
//    HbaseUtils.put("TagsF","Day01","PersonTags",hashMap)

    //    存储到es
    val mapEsTags: RDD[(String, String)] = logTags.map(tunpl => {
      (tunpl._1.toString -> tunpl._2.toString())
    })
//    mapEsTags.collect().foreach(println)
    mapEsTags.saveToEs("tags/doc")


//scala创建一个类的时候会自动执行类中的所有语句

    spark.stop()

  }

  def getUserIDs(row: Row): List[String] = {
    val fields: String = ConfigHelper.idFields
    fields.split(",")
      .map(field => (field, row.getAs[String](field)))
      .filter{case (key, value) => StringUtils.isNotBlank(value)}
      .map{case (key, value) => s"$key::$value"}
      .toList
  }
}
