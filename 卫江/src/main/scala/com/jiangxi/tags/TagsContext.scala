package com.jiangxi.tags

import com.jiangxi.entry.logBean
import com.jiangxi.util.{TagsUtils, rdd2hbase}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.mutable

object TagsContext {
  //E:\\DMPData\\Outpath\\out_parquet_data C:\\Users\\Administrator\\Desktop\\josn.txt
  def main(args: Array[String]): Unit = {
    /**
      * 第一步判断参数个数
      */
    if(args.length < 2){
      println(
        """
          |com.dmp.total.ProvniceCityAnlyse <inputFilePath><outputFilePath>
          |<inputFilePath> 输入是文件路径
          |<outputFilePath> 输出的文件路径
        """.stripMargin)
      System.exit(0)
    }

    /**
      * 第二步接收参数
      */
    val Array(inputFile,outputFile)=args
    /**
      * 第三步初始化程序入口
      */


    val spark=SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName }")
      .master("local[4]")
      .config("spark.debug.maxToStringFields", "100")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "node245:9200")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    //app详解数据
    val appDetailData: RDD[String] = spark.sparkContext.textFile("E:\\DMPData\\Inpath\\app_dict.txt")
    //准备广播数据集
    //对数据进行过滤
    val app_id_name: Array[(String, String)] = appDetailData.filter(line => {
      line.split("\t").length >= 5 && line.split("\t")(3).startsWith("A")
    }).map(line => {
      val arr: Array[String] = line.split("\t")
      val appname = arr(1)
      val appid = arr(2)
      (appid, appname)
    }).collect()
    //获取stopword数据（电视剧的种类）
    val stopwordArr: Array[String] = spark.sparkContext.textFile("E:\\DMPData\\Inpath\\stopwords.txt").collect()
    //进行广播
    val app_id_name_Bc: Broadcast[Array[(String, String)]] = spark.sparkContext.broadcast(app_id_name)
    val stopwordBc: Broadcast[Array[String]] = spark.sparkContext.broadcast(stopwordArr)

    //加载需要处理的数据
    val frame: DataFrame = spark.read.parquet(inputFile)
    val alltags: RDD[(String, List[(String, Double)])] = frame.filter(TagsUtils.userIdOne).rdd.map(line => {
      //.substring(0, line.toString().length - 1)
      val log: logBean = logBean.line2log(line.toString())
      //广告标签
      val adtag = adTag.makeTag(log)
      //app标签
      val apptag: List[(String, Double)] = appTag.makeTag(log, app_id_name_Bc)
      //渠道标签
       val channeltag: List[(String, Double)] = channelTag.makeTag(log)
      //设备标签
      val devicetag: List[(String, Double)] = deviceTag.makeTag(log)
      //关键字标签
      val keyWordstag: List[(String, Double)] = keyWordsTag.makeTag(log)
      //地域标签
      val areatag: List[(String, Double)] = areaTag.makeTag(log)
      //商圈标签
      //val businesstag: List[(String, Int)] = businessTag.makeTag(log)
      //得到userID
      //val userid: String = TagsUtils.getAllUserIds(log)
      val userid:String = TagsUtils.getAllUserId(line).mkString(",")
      val tags = adtag ++ apptag ++ channeltag  ++ devicetag ++ keyWordstag ++ areatag //++businesstag
      (userid, tags)
    })


    //println(alltags.count())  //39156
    //接下来需要构建图，所谓的构建图就是将用户的各种行为聚合在一起，例如（1，2）（各种标签） --（1，3）（各种标签）====>(1,2,3) (聚合后的各种标签)
    //Spark GraphX是一个分布式的图处理框架，大体流程是构建点所在的rdd和边所在的rdd,然后进行点边聚合形成图
    // 每一个点集合之中只能有一个点携带标签，其他的点不需要携带标签，如果其他的点也携带了标签
    // 那么此时数据就会发生变化，造成重复现在，为了避免这种问题，我们就得保证一个点携带标签//(List[String], List[(String, Int)])
    //先获取图的点集合
    val vertexRDD: RDD[(Long, (String, List[(String, Double)]))] = alltags.map(line => {
      var arr = line._1.split(",")
      (arr(0).hashCode.toLong, line)
      /*    arr.map(uid=>{
          if(arr(0).equals(uid)) {
            (uid.toLong, line._2)
          }else{
            (uid.toLong,List.empty)
          }
        })*/
    })
    //其次获取图的边集合
    val edge: RDD[Edge[Int]] = alltags.flatMap(line => {
      var arr = line._1.split(",")
      arr.map(uid => {
        Edge(arr(0).hashCode.toLong, uid.hashCode.toLong, 0)
      })
    })
    //构建图
    val graph: Graph[(String, List[(String, Double)]), Int] = Graph(vertexRDD,edge)
    // 取顶点ID
    val comm = graph.connectedComponents().vertices

    val tujisuan: RDD[(VertexId, (String, List[(String, Double)]))] = comm.join(vertexRDD).map {
      //cmId为图处理框架为你判断的id，id代表聚合以后统一的id
      case (userid, (cmId, list)) => (cmId, list)
    }
    val boss: RDD[(String, List[(String, Double)])] = tujisuan.map(line => {
      val key: String = line._1.toString
      val value: List[(String, Double)] = line._2._2
      (key, value)
    }).reduceByKey(
      (list1, list2) => (list1 ++ list2)
        // list((a,1),(b,1),(c,1),(d,1),(a,1),(b,1),(a,1))
        // map[a,List[(a,1),(a,1),(a,1)]]
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList)
    //boss.foreach(println)
    //println(boss.count())  //29960

     //存入es
     import org.elasticsearch.spark._
     //boss.saveToEs("dmptags/tags")


  //(IM: -1,List((LC12,1), (LN视频前贴片,1), (APP爱奇艺,1), (CN8,1), (D00010001,1), (D00020005,1), (D00030004,1), (K游戏世界,1), (K单机游戏,1), (ZP浙江省,1), (ZC温州市,1)))
    //首先过滤掉等于空的userid
    //这里是根据用户id进行了标签聚合
/*  val result: RDD[(String, List[(String, Int)])] = alltags.filter(!_._1.isEmpty).reduceByKey {
      case (list1, list2) => {
        //这里的逻辑验证在test里面验证，更加明了
        (list1 ++ list2).groupBy(_._1)
          .map {
            case (key, list) => {(key, list.map(x => x._2).sum)}
          }.toList
      }
    }*/
   // result.foreach(println)


    //存入Hbase，表需要提前先建好
    val tablename :String ="lhh_test:tags"
    val hbconf: Configuration = HBaseConfiguration.create()
    hbconf.set("hbase.zookeeper.quorum","node245,node246,node247")
    hbconf.set("hbase.zookeeper.property.clientPort", "2181")
    hbconf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    hbconf.set(TableInputFormat.INPUT_TABLE,tablename)

    //初始化jobconf
    val jobConf = new JobConf(hbconf)
    jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Put])
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    val hbaseres: RDD[(ImmutableBytesWritable, Put)] = boss.map {
      case (userid, tagList) => {
        //rowkey,如果是数值类型，转化成字符串，可能出现乱码
        //Put.add方法接收三个参数：列族，列名，数据
        val put = new Put(Bytes.toBytes(userid.toString))
        val tags = tagList.map(item => item._1 + ":" + item._2).mkString(",")
        put.addImmutable(Bytes.toBytes("cf1"), Bytes.toBytes("day1"), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }
    }
      hbaseres.saveAsHadoopDataset(jobConf)
      //读取hbase的数据
      val sc: SparkContext = spark.sparkContext
      //读hbase中的数据到spark RDD
      val getRdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],
        classOf[Result]
      )
    //RDD[(String, List[(String, Int)])]
    //指定从hbase读出来的数据格式和今天的数据格式一样
    val yesterdayData: RDD[(String, List[(String, Double)])] = getRdd.map({ case (_, result) =>

      val rowkey = Bytes.toString(result.getRow)
      val tags = Bytes.toString(result.getValue("cf1".getBytes, "day1".getBytes))
      val ins: List[(String, Double)] = tags.split(",").map(line => {
        val arr: Array[String] = line.split(":")
        (arr(0), arr(1).toDouble)
      }).toList
      //println(rowkey+":"+tags)
      (rowkey: String, ins: List[(String, Double)])
    })
    //yesterdayData.foreach(println)
    //做标签衰减 （标签衰减（每一个标签，都有权重，代表个人的匹配程度））
    //将今天的数据和昨天的数据进行标签聚合，这里用同样的数据模拟实现
  /*  val shuaijian: RDD[(String, List[(String, Double)])] = boss.union(yesterdayData)
      .reduceByKey(
        (list1, list2) => (list1 ++ list2))
      .map(x => {
        val list: List[(String, Double)] = x._2.groupBy(_._1)
          .mapValues(_.foldLeft(0.0)(_ * 0.8 + _._2))
          .toList
        (x._1, list)
      })
   shuaijian.foreach(println)*/
     boss.union(yesterdayData).reduceByKey(
        (list1, list2) => (list1 ++ list2)
          // list((a,1),(b,1),(c,1),(d,1),(a,1),(b,1),(a,1))
          // map[a,List[(a,1),(a,1),(a,1)]]
          .groupBy(_._1)
          .mapValues(_.foldLeft(0.0)(_ * 0.8 + _._2))
          .toList).foreach(println)

    spark.stop()
}

}
