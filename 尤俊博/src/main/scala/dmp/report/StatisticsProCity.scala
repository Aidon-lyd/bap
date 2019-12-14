package dmp.report

import java.util.Properties

import dmp.util.ConfigHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object StatisticsProCity {

  def getProCityToDisk(spark:SparkSession): Unit ={
    //定义参数
    val sourceDataFile: String = ConfigHelper.adFormatData  //输入路径
    val sinkData:String = ConfigHelper.outputJson //输出路径

    import spark.implicits._
    val data: DataFrame = spark.read.parquet(sourceDataFile).cache()

    // spark sql 方式
//    data.createOrReplaceTempView("ods_dmp")
//    spark.catalog.cacheTable("ods_dmp")
//
//    //sql
//    val proCityData: DataFrame = spark.sql(
//      """
//        |select
//        | count(1) as cy,
//        | provincename,
//        | cityname
//        |from ods_dmp
//        |group by provincename, cityname
//      """.stripMargin)
//
//    proCityData.coalesce(1).write.json(sinkData)

    val data_Pro_City: DataFrame = data.groupBy("provincename", "cityname")
      .count()
      .withColumnRenamed("count", "ct")
      .cache()

    data_Pro_City.coalesce(1).write.json(sinkData)
  }

  def getProCityToMysql(spark: SparkSession) = {
    //定义参数
    val sourceDataFile: String = ConfigHelper.adFormatData  //输入路径
    val connectionProperties: Properties = new Properties()
    connectionProperties.put("driver","com.mysql.jdbc.Driver")
    connectionProperties.put("user","root")
    connectionProperties.put("password","12345678")

    import spark.implicits._
    val data: DataFrame = spark.read.parquet(sourceDataFile).cache()

    import  org.apache.spark.sql.functions._
    val data_Pro_City: DataFrame = data.groupBy("provincename", "cityname")
      .count()
      .withColumnRenamed("count", "ct")
      .selectExpr("provincename","cityname","ct")

    //data_Pro_City.coalesce(1).show(10)
    //
    //往数据库写入数据注意数据库的字符编码
//    data_Pro_City.write.mode("append").jdbc("jdbc:mysql://10.0.88.245:3306/adv?useUnicode=true&characterEncoding=utf-8","ProCity",connectionProperties)


//    //rdd方式
//    val tunlp: RDD[((String, String), Int)] = data.rdd.mapPartitions(item => {
//      item.map(
//        line => {
//          val provincename: String = line.getString(24)
//          val cityname: String = line.getString(25)
//          ((provincename, cityname), 1)
//        }
//      )
//    })
//
//    val result: RDD[((String, String), Int)] = tunlp.reduceByKey(_+_)
////    result.saveAsTextFile("  ")  //存入磁盘
//    result.collect().foreach(print) //输出
  }
}
