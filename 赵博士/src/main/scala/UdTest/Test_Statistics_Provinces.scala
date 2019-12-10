package UdTest


import Exceptions.SparkSessionInitException
import Global_Fields.{TableName_ODS, TableName_Test}
import Spark_Utils.{SparkSession_Utils, Spark_Option_Util}
import ods_release.ODS_Relase
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/6
  * Time: 21:04
  *
  * @author zbs
  * @version 1.0
  */
object Test_Statistics_Provinces {
  val LEVEL = StorageLevel.MEMORY_AND_DISK
  val TEXT_File_Type = "text"
  val PARQUET_File_Type = "parquet"
  val JSON_File_Type = "json"
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  /**
    * @Author zbs
    * @Description:构建Test_Statistics_Provinces
    * @Date: 2019/12/7
    * @Param spark:
    * @return: void
    */
  def test_Stat_Dispose(spark: SparkSession): Unit = {
    try {
      if (spark == null) {
        throw new SparkSessionInitException("spark初始化异常")
      }
      import spark.implicits._
      val ODS_Df: DataFrame = ODS_Relase.ods_Dispose(spark)
      //分组聚合，持久化
      val Stat_pro = ODS_Df.selectExpr(Test_Fields.procinces_files: _*)
        .groupBy(Test_Fields.GROUP_FIELDS_PROVIN, Test_Fields.GROUP_FIELDS_CITY)
        .agg(
          countDistinct(Test_Fields.GROUP_FIELDS_SESSIONID) as (Test_Fields.GROUP_FIELDS_COUNTDISTIN)
        )
        .repartition(5)
//        .persist(LEVEL)
      //排序查询显示
      val sort_Stat_Pro: Dataset[Row] = Stat_pro.selectExpr(Test_Fields.group_Fields: _*)
        .sort(col(Test_Fields.GROUP_FIELDS_COUNTDISTIN) desc)
      sort_Stat_Pro.persist(LEVEL)
        .show(100)
      //将df以json形式写入磁盘
      Spark_Option_Util
        .writeFile(sort_Stat_Pro, TableName_Test.Table_Test_Json_Path, JSON_File_Type)
     //将df写入mysql
      Spark_Option_Util
        .spark_JDBC_mysql(sort_Stat_Pro, TableName_Test.SAVE_MODE)
    }
    catch {
      case ex: Exception => {
        println(s"聚合地域分布test时,error")
        logger.error(s"聚合地域分布test时,error:\n${ex.getMessage}")
      }
    }
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getName)
    val spark: SparkSession = SparkSession_Utils.getSpark(conf)
    test_Stat_Dispose(spark)
  }

}
