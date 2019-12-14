package com.jiangxi.Original

import com.jiangxi.entry.logBean
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DMP_Count {

  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf()
      .setAppName("DMP_Count")
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[logBean]))
    //spark.io.compression.codec=snappy
    //spark上下文会话
    val sc: SparkContext = new SparkContext(sconf)
    val spark: SparkSession = SparkSession.builder().config(sconf).getOrCreate()
    import org.apache.spark.sql.functions._
    //获取数据
  val frame: DataFrame = spark.read.csv("E:\\DMPData\\Inpath\\2016-10-01_06_p1_invalid.1475274123982.log")
    frame
      .select("_c22","_c23").show(1000000)
    /* val frame: DataFrame = data.map(x => x.split(","))
      .filter(x => x.length == 85)
      .map(line =>
        logBean(
          line(0),
          line(1).toInt,
          line(2).toInt,
          line(3).toInt,
          line(4).toInt,
          /*if(line(1).isEmpty) -1 else line(1).toInt,
          if(line(2).isEmpty) -1 else line(2).toInt,
          if(line(3).isEmpty) -1 else line(3).toInt,
          if(line(4).isEmpty) -1 else line(4).toInt,*/
          line(5),
          line(6),
          line(7).toInt,
          line(8).toInt,
          line(9).toDouble,
          line(10).toDouble,
          /*if(line(7).isEmpty) -1 else line(7).toInt,
          if(line(8).isEmpty) -1 else line(8).toInt,
          if(line(9).isEmpty) -1.0 else line(9).toDouble,
          if(line(10).isEmpty) -1.0 else line(10).toDouble,*/
          line(11),
          line(12),
          line(13),
          line(14),
          line(15),
          line(16),
          line(17).toInt,
          //if(line(17).isEmpty) -1 else line(17).toInt,
          line(18),
          line(19),
          /*line(20).toInt,
          line(21).toInt,*/
          if (line(20).isEmpty) -1 else line(20).toInt,
          if (line(21).isEmpty) -1 else line(21).toInt,
          line(22),
          line(23),
          line(24),
          line(25),
          line(26).toInt,
          //if(line(26).isEmpty) -1 else line(26).toInt,
          line(27),
          line(28).toInt,
          //if(line(28).isEmpty) -1 else line(28).toInt,
          line(29),
          line(30).toInt,
          line(31).toInt,
          line(32).toInt,
          /*if(line(30).isEmpty) -1 else line(30).toInt,
          if(line(31).isEmpty) -1 else line(31).toInt,
          if(line(32).isEmpty) -1 else line(32).toInt,*/
          line(33),
          line(34).toInt,
          line(35).toInt,
          line(36).toInt,
          /*if(line(34).isEmpty) -1 else line(34).toInt,
          if(line(35).isEmpty) -1 else line(35).toInt,
          if(line(36).isEmpty) -1 else line(36).toInt,*/
          line(37),
          line(38).toInt,
          line(39).toInt,
          line(40).toDouble,
          line(41).toDouble,
          line(42).toInt,
          /*if(line(38).isEmpty) -1 else line(38).toInt,
          if(line(39).isEmpty) -1 else line(39).toInt,
          if(line(40).isEmpty) -1.0 else line(40).toDouble,
          if(line(41).isEmpty) -1.0 else line(41).toDouble,
          if(line(42).isEmpty) -1 else line(42).toInt,*/
          line(43),
          line(44).toDouble,
          line(45).toDouble,
          /*if(line(44).isEmpty) -1.0 else line(44).toDouble,
          if(line(45).isEmpty) -1.0 else line(45).toDouble,*/
          line(46),
          line(47),
          line(48),
          line(49),
          line(50),
          line(51),
          line(52),
          line(53),
          line(54),
          line(55),
          line(56),
          line(57).toInt,
          line(58).toDouble,
          line(59).toInt,
          line(60).toInt,
          /*if(line(57).isEmpty) -1 else line(57).toInt,
          if(line(58).isEmpty) -1.0 else line(58).toDouble,
          if(line(59).isEmpty) -1 else line(59).toInt,
          if(line(60).isEmpty) -1 else line(60).toInt,*/
          line(61),
          line(62),
          line(63),
          line(64),
          line(65),
          line(66),
          line(67),
          line(68),
          line(69),
          line(70),
          line(71),
          line(72),
          line(73).toInt,
          line(74).toDouble,
          line(75).toDouble,
          line(76).toDouble,
          line(77).toDouble,
          line(78).toDouble,
          /*if(line(73).isEmpty) -1 else line(73).toInt,
          if(line(74).isEmpty) -1.0 else line(74).toDouble,
          if(line(75).isEmpty) -1.0 else line(75).toDouble,
          if(line(76).isEmpty) -1.0 else line(76).toDouble,
          if(line(77).isEmpty) -1.0 else line(77).toDouble,
          if(line(78).isEmpty) -1.0 else line(78).toDouble,*/
          line(79),
          line(80),
          line(81),
          line(82),
          line(83),
          line(84).toInt)
        //if(line(84).isEmpty) -1 else line(84).toInt)
      )
      .toDF()
    frame.write.parquet("E:\\千峰培训\\spark离线\\DMP离线项目\\DMP项目资料\\out_parquet111")*/

    val frame01: DataFrame = spark.read.parquet("E:\\千峰培训\\spark离线\\DMP离线项目\\DMP项目资料\\out_parquet")
  frame01
    .select(
    "long1",
    "lat"
  ).filter("long1!='' and long1!=0 ")
    //.show(1000)
    val dataFrame: DataFrame = frame01
      .groupBy("provincename", "cityname")
      .count().withColumnRenamed("count", "ct")
      .cache()
    //需求一
    dataFrame.repartition(1)
       // .write.json("C:\\Users\\Administrator\\Desktop\\josn.txt")
    //需求二
    //SparkHelper.writeToMysql(dataFrame,"dmp_count")
//    expr("SUM(CASE WHEN requestmode=1 and processnode>=2 THEN 1 ELSE 0 END) as validRequest"),
    //地域分布
    frame01
      .groupBy("provincename", "cityname")
      .agg(
        expr("sum(case when requestmode=1 and processnode>=1 THEN 1 ELSE 0 END) as OriginalRequest"),
        expr("sum(case when requestmode=1 and processnode>=2 THEN 1 ELSE 0 END) as effectRequest"),
        expr("sum(case when requestmode=1 and processnode=3 THEN 1 ELSE 0 END) as advertiserRequest"),
        expr("sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISBID=1 THEN 1 ELSE 0 END) as bidding_count"),
        expr("sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 and ADORDERID !=0 THEN 1 ELSE 0 END) as bidding_success"),
        expr("sum(case when requestmode=2 and ISEFFECTIVE=1 THEN 1 ELSE 0 END) as show_count"),
        expr("sum(case when requestmode=3 and ISEFFECTIVE=1 THEN 1 ELSE 0 END) as client_count"),
        expr("sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 THEN WinPrice ELSE 0 END)/1000 as DSP_ADconsume"),
        expr("sum(case when requestmode=1 and processnode>=1 THEN adpayment ELSE 0 END)/1000 as DSP_ADcost")
      )
      .selectExpr(
         "provincename as `省市`",
        "cityname as `城市`",
        "OriginalRequest as `原始请求`",
        "effectRequest as `有效请求`",
        "advertiserRequest as `广告请求`",
        "bidding_count as `参与竞价数`",
        "bidding_success as `竞价成功数`",
        "bidding_success/bidding_count as `竞价成功率`",
        "show_count as `展示量`",
        "client_count as `点击量`",
        "show_count/client_count as `点击率`",
        "DSP_ADconsume as `广告成本`",
        "DSP_ADcost as `广告消费`"
        )//.show(100)

    spark.stop()

  }
}
