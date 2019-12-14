package com.jiangxi.DMP

import com.jiangxi.entry.logBean
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}


object ParquetFile {
  //E:\\DMPData\\Inpath\\2016-10-01_06_p1_invalid.1475274123982.log.FINISH
  //E:\\DMPData\\Outpath\\out_parquet_data
  //snappy
  def main(args: Array[String]): Unit = {
    /**
      * 第一步：判断参数是否符合需求
      * 原始的文件路径 输出的文件路径 压缩格式
      */
    if (args.length < 3) {
      println(
        """
          |com.dmp.total.Txt2Parquet <dataPath> <outputPath> <compressionCode>
          |<dataPath>:日志所在的路径
          |<outputPath>：结果文件存放的路径
          |<compressionCode>：指定的压缩格式
        """.stripMargin)
      System.exit(0)
    }
    /**
      * 第二步：接收参数
      */
    val Array(dataPath, outputPath, compressionCode) = args

    /**
      * 第三步：创建SparkSession对象
      */
    val sconf = new SparkConf()
      .setAppName("DMP_Count")
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[logBean]))  //指定序列化的格式为kryo
      .set("spark.io.compression.codec",compressionCode)
    //spark.io.compression.codec=snappy
    val sc: SparkContext = new SparkContext(sconf)
    val spark: SparkSession = SparkSession.builder().config(sconf).getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    /**
      * 第四步：读取文件，对文件做相对应的操作
      */
      val logRDD: RDD[logBean] = sc.textFile(dataPath).map(line => logBean.line2log(line))
     val frame: DataFrame = logRDD.toDF()
    /**
      * 第五步：指定文件存放的位置
      */
    //.partitionBy("provincename","cityname")
    frame.write.parquet(outputPath)

    spark.stop()

  }
}
