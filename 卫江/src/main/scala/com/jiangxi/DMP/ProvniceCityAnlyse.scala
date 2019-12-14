package com.jiangxi.DMP

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProvniceCityAnlyse {

  def main(args: Array[String]): Unit = {
    //E:\\DMPData\\Outpath\\out_parquet_data
    //C:\\Users\\Administrator\\Desktop\\josn.txt
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
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName }")
    conf.setMaster("local[4]")
    val spark=SparkSession.builder()
      .config(conf)
      .getOrCreate()

    /**
      * 第四步读取文件，进行业务逻辑开发
      * 云南省：
      * 云南省     曲靖市
      * 云南省     昆明市
      * 云南省     大理市
      */
    val frame01: DataFrame = spark.read.parquet(inputFile)
frame01.show(100)
    val dataFrame: DataFrame = frame01
      .groupBy("provincename", "cityname")
      .count().withColumnRenamed("count", "ct")
      .cache()

   // dataFrame .show(100)
    //需求一
    dataFrame.repartition(1)
      //.write.json(outputFile)
    // .write.json("C:\\Users\\Administrator\\Desktop\\josn.txt")
    //需求二
    //SparkHelper.writeToMysql(dataFrame,"dmp_count")
    spark.stop()
  }
}
