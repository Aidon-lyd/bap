package dmp

import dmp.bean.Log
import dmp.etl.ETLProcessor
import dmp.report.{Location, StatisticsProCity}
import dmp.tags.{TagContext, TagContextV2}
import dmp.tradingarea.TradingAreaProcessor
import dmp.util.ConfigHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DMPApp {
  def main(args: Array[String]): Unit = {
    //初始化
    val conf: SparkConf = new SparkConf()
      .setMaster(ConfigHelper.sparkMaster)
      //指定序列化的类与序列化方式，可以减少IO的传输大小,但是spark sql的dataframe和dataset内部自己实现了比较好的java的序列化方式，所以kyro只需要指定自己创建的bean类型
      .registerKryoClasses(Array(classOf[Log]))
      .setAppName(ConfigHelper.sparkAppName)
      .setAll(ConfigHelper.sparkParameters)

    val sparkSession: SparkSession = SparkSession.builder()
      .config(conf)
      //.enableHiveSupport()
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("error")

    //ETL
//    ETLProcessor.processor(sparkSession)

    //report
//    StatisticsProCity.getProCityToDisk(sparkSession)
//    StatisticsProCity.getProCityToMysql(sparkSession)

    //地域分布
//    Location.getLocation(sparkSession)

    //将每日的商圈信息存入redis数据库
//    TradingAreaProcessor.processor(sparkSession)

    //保存标签
//    TagContext.processor(sparkSession)

    //保存标签，构建图，存储到hbase和es
    TagContextV2.processor(sparkSession)

  }
}
