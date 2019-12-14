package com.cmcc.dmp.Utils

import com.cmcc.dmp.filehand.DmpLOG
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * requirement  Spark工具类
  *
  * @author zhangsl
  * @date 2019/12/6 14:14 
  * @version 1.0
  */
object SparkHelper {

  // 获取SparkConf
  def getSparkConf(appName:String,master:String): SparkConf={
    return new SparkConf().setAppName(appName).setMaster(master)
  }

  // 通过SparkConf对象获取SparkSession对象
  def getSpark(conf:SparkConf):SparkSession = {
    return SparkSession.builder().config(conf).getOrCreate()
  }


}
