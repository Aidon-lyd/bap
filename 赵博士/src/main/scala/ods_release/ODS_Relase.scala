package ods_release

import Exceptions.NullDataframeException
import Global_Fields.TableName_ODS
import Spark_Utils.{SparkSession_Utils, Spark_Option_Util}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/6
  * Time: 16:07
  *
  * @author zbs
  * @version 1.0
  */
object ODS_Relase {
  val LEVEL = StorageLevel.MEMORY_AND_DISK
  val TEXT_File_Type = "text"
  val PARQUET_File_Type = "parquet"
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getName)
    val spark: SparkSession = SparkSession_Utils.getSpark(conf)
    ods_Dispose(spark)
    SparkSession_Utils.close(spark)
  }

  /**
    * @Author zbs
    * @Description:构建ODS层
    * @Date: 2019/12/6
    * @return: java.lang.Object
    */
  def ods_Dispose(spark: SparkSession) = {
    var format_Ds: DataFrame=null
    try {
      Spark_Option_Util
        .writeFile(Spark_Option_Util.getDf_File(spark, TEXT_File_Type, TableName_ODS.FILE_NAME), TableName_ODS.PATQUET_FILE_PATH, PARQUET_File_Type)

      format_Ds =
        format_Data(spark,Spark_Option_Util.getDf_File(spark, PARQUET_File_Type, TableName_ODS.PATQUET_FILE_PATH))
//      SparkSession_Utils.Refresh_Table(spark,format_Ds)
      format_Ds.persist(LEVEL)
    }
    catch {
      case ex: Exception => {
        println(s"构建ods层时,error")
        logger.error(s"构建ods层时,error:\n${ex.getMessage}")
      }
    }
    format_Ds
  }



  /**
    * @Author zbs
    * @Description:将初始数据转换成85列df
    * @Date: 2019/12/6
    * @Param spark:
    * @Param InitDs:
    * @return:DataSet
    */
  def format_Data(spark: SparkSession, InitDs: Dataset[Row]) = {
    import spark.implicits._
    var format_Ds: DataFrame = null
    try {
      if (InitDs == null) {
        throw new NullDataframeException(s"传入数据集为空")
      }
      format_Ds = InitDs
        .map(prelin => {
          val strings: Array[String] = prelin.getString(0).split(",", -1)
          strings
        })
        .filter(prelin => prelin.length == 85)
        .selectExpr(ODS_Fields.InitArr_Fields: _*)
    }
    catch {
      case ex: Exception => {
        println("将初始数据转换成85列df时,error")
        logger.error(s"将初始数据转换成85列df时,error:\n${ex.getMessage}")
      }
    }
    format_Ds.persist(LEVEL)
  }

}
