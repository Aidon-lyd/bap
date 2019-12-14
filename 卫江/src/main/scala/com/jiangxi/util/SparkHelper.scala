package com.jiangxi.util

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object SparkHelper {
  val logger :Logger = LoggerFactory.getLogger(SparkHelper.getClass)
  //缓存级别
  var storageLevels :mutable.Map[String,StorageLevel] = mutable.Map[String,StorageLevel]()
  /**
    * 创建支持hive的sparkSession
    * @param
    * @return
    */
  def createSpark(sconf:SparkConf) :SparkSession = {
    val spark :SparkSession = SparkSession.builder
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate();
    spark
  }

  /**
    * 创建不支持hive的sparkSession
    * @param sconf
    * @return
    */
  def createSparkNotHive(sconf:SparkConf) :SparkSession = {
    val spark :SparkSession = SparkSession.builder
      .config(sconf)
      .getOrCreate();
    spark
  }
  /**
    * mysql存储聚合数据
    * @param spark
    */
  def writeToMysql(df:DataFrame, tableName:String): Unit ={
    //mysql
    val connectionProperties = new Properties()
    connectionProperties.put("user","root")
    connectionProperties.put("password","root")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    if(null !=connectionProperties ){
      df.write.mode(SaveMode.Overwrite)
        .jdbc("jdbc:mysql://localhost:3306/mao", tableName,connectionProperties)
    }
  }
}
