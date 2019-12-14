package com.jiangxi.DMP

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object AreaDistribution {
  def main(args: Array[String]): Unit = {
    //E:\\DMPData\\Outpath\\out_parquet_data
    //C:\\Users\\Administrator\\Desktop\\josn.txt
    /**
      * 第一步判断参数个数
      */
    if (args.length < 2) {
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
    val Array(inputFile, outputFile) = args
    /**
      * 第三步初始化程序入口
      */
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[4]")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val frame01: DataFrame = spark.read.parquet(inputFile)
    //longitude.toDouble!=0&lat.toDouble!=0&longitude.toDouble!=-1&lat.toDouble!=-1
    frame01
      .show(1000000)
    //地域分布
    val frame: DataFrame = frame01
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
      )
    //frame.show(100)
    spark.stop()
  }
}
