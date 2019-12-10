package Spark_Utils


import Exceptions.SparkSessionInitException
import Spark_Utils.Spark_Option_Util.logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}


/**
  * spark的工具类
  * User: zbs
  * Date: 2019/12/6
  * Time: 13:54
  *
  * @author zbs
  * @version 1.0
  */
object SparkSession_Utils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @Author zbs
    * @Description:根据Sparkconf创建sparkSession
    * @Date: 2019/12/6
    * @Param conf:
    * @return: org.apache.spark.sql.SparkSession
    */
  def getSpark(conf: SparkConf) = {
    var spark: SparkSession = null
    try {
      conf
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.hive.convertMetastoreParquet", "true")
        .set("spark.sql.parquet.compression.codec", "snappy")
        .set("spark.sql.parquet.filterPushdown", "true")
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      spark = SparkSession
        .builder()
        .enableHiveSupport()
        .config(conf)
        .getOrCreate()
    }
    catch {
      case ex: Exception => {
        logger.error(s"conf创建sparksession时错误:\n${ex.getMessage}")
      }
//      case _ => {
//        logger.error(s"conf创建sparksession时错误:\n${_.getMessage}")
//      }
    }
    spark
  }

  /**
    * @Author zbs
    * @Description:udf注册
    * @Date: 2019/12/6
    * @Param spark:
    * @return: org.apache.spark.sql.SparkSession
    */
  def register(spark: SparkSession) = {
    try {
      if (spark == null) {
        throw new SparkSessionInitException("[error]conf创建sparksession时错误")
      }
      spark.udf.register("format", Spark_Hive_Udfs.formatData _)
    }
    catch {
      case ex: SparkSessionInitException=>{
        logger.error(s"[error]spark注册Udf:\n${ex.getStackTrace}")
      }
    }
    spark
  }

  /**
    * @Author zbs
    * @Description:整合hive和spark的parquet的不同
    * @Date: 2019/12/6
    * @Param spark:
    * @Param tablename:
    * @return: void
    */
  def Refresh_Table(spark: SparkSession, tablename: String) = {
    try {
      if (spark == null) {
        throw new SparkSessionInitException("[error]conf创建sparksession时错误")
      }
      spark.catalog.refreshTable(tablename)
    }
    catch {
      case ex: SparkSessionInitException=>{
        logger.error(s"[error]spark整合hive和spark的parquet的不同时 error:\n${ex.getStackTrace}")
      }
    }
    spark
  }

  /**
    * @Author zbs
    * @Description:注销sparkSession
    * @Date: 2019/12/6
    * @Param spark:
    * @return: void
    */
  def close(spark: SparkSession) = {
    if (spark != null) {
      spark.stop()
    }
    else {
      println("sparkSession未初始化")
      logger.error("conf创建sparksession时错误")
    }
  }

}
