package com.cmcc.dmp.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * requirement  分区测试
  *
  * @author zhangsl
  * @date 2019/12/12 21:45 
  * @version 1.0
  */
object PartitionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[Char] = context.parallelize("log/dick/app_dict.txt",300)

    rdd.saveAsTextFile("testPar")

    context.stop()

  }
}
