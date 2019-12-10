package Spark_Utils


import java.io.{PrintWriter, StringWriter}
import java.util.Properties

import Exceptions.{NullConfException, NullDataframeException, SparkRaedTableException, SparkSessionInitException}
import Global_Fields.JDBC_FIELDS
import Spark_Utils.Spark_Http_Util.logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/6
  * Time: 15:23
  *
  * @author zbs
  * @version 1.0
  */
object Spark_Option_Util {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @Author zbs
    * @Description:简单的读取spark on hive读取表
    * @Date: 2019/12/6
    * @Param spark:
    * @Param tablename:
    * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    */
  def simpleReadtable(spark: SparkSession, tablename: String) = {
    try {
      if (spark == null) {
        throw new SparkSessionInitException("[error]conf创建sparksession时错误")
      }
      spark.read.table(tablename)
    }
    catch {
      case ex: SparkSessionInitException => {
        logger.error(s"[error]spark读表数据时错误:\n${ex.getMessage}")
      }
    }
  }

  /**
    * @Author zbs
    * @Description:读取hive/local的表
    * @Date: 2019/12/6
    * @Param spark:
    * @Param tablename:
    * @Param format:
    * @Param option:
    * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    */
  def readTable(spark: SparkSession, tablename: String, format: String = null, option: Map[String, String]) = {
    var df: DataFrame = null
    try {
      if (spark == null) {
        throw new SparkRaedTableException("[error]conf创建sparksession时错误\n")
      }
      val reader: DataFrameReader = spark.read.format(format)
      option.map(prelin => {
        reader.option(prelin._1, prelin._2)
      })
      df = reader.table(tablename)
    }
    catch {
      case ex: Exception => {
        logger.error(s"spark读表数据时出错:\n${ex.getMessage}")
      }
    }
    df
  }

  /**
    * @Author zbs
    * @Description:读取文件
    * @Date: 2019/12/6
    * @Param spark:
    * @Param fileName:
    * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    */
  def readFile(spark: SparkSession, fileName: String, op: String) = {
    var df: DataFrame = null
    try {
      if (spark == null) {
        throw new SparkRaedTableException("[error]conf创建sparksession时错误\n")
      }
      df = spark.read.format(op).load(fileName)
    }
    catch {
      case ex: Exception => {
        logger.error(s"spark读取${op}数据出错:\n${ex.getMessage}")
      }
    }
    df
  }

  /**
    * @Author zbs
    * @Description:写入文件
    * @Date: 2019/12/6
    * @Param df:
    * @Param fileName:
    * @Param op:
    * @return: Unit
    */
  def writeFile(df: DataFrame, fileName: String, op: String) = {
    try {
      if (df == null) {
        throw new NullDataframeException("df空值传入 error\n")
      }
      df.write.format(op).save(fileName)
    }
    catch {
      case ex: Exception => {
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        ex.printStackTrace(pw)
        logger.error(s"spark读取${op}数据出错:\n${sw.toString}")
      }
    }
  }

  /**
    * @Author zbs
    * @Description:将df写入表的save
    * @Date: 2019/12/6
    * @Param df:
    * @Param mode:
    * @Param tableName:
    * @return: void
    */
  def writeTable(df: DataFrame, mode: SaveMode, tableName: String) = {
    try {
      if (df == null) {
        throw new NullDataframeException("df空值传入 error\n")
      }
      df.write.mode(mode).saveAsTable(tableName)
    }
    catch {
      case ex: Exception => {
        logger.error(s"spark写入${mode}模式hive表出错:\n${ex.getMessage}")
      }
    }
  }

  /**
    * @Author zbs
    * @Description:注册序列化kyro类（未使用）
    * @Date: 2019/12/7
    * @Param conf:
    * @return: org.apache.spark.SparkConf
    */
  def makeKyro(conf: SparkConf) = {
    try {
      if (conf == null) {
        throw new NullConfException("conf未赋值")
      }

    }
    catch {
      case ex: Exception => {
        logger.error(s"spark注册序列化类时出错:\n${ex.getMessage}")
      }
    }
    conf
  }
  /**
    * @Author zbs
    * @Description:读取文件获得初始化ds
    * @Date: 2019/12/6
    * @return: org.apache.spark.sql.Dataset
    */
  def getDf_File(spark: SparkSession, op: String, File_name: String) = {
    var Initds: Dataset[Row] = null
    try {
      Initds = Spark_Option_Util
        .readFile(spark, File_name, op)
    }
    catch {
      case ex: Exception => {
        println(s"读取${op}文件${File_name}时，error")
        logger.error(s"读取${op}文件${File_name}时，error:\n${ex.getMessage}")
      }
    }
    Initds
  }
  /**
    * @Author zbs
    * @Description:将数据写入mysql
    * @Date: 2019/12/7
    * @Param spark:
    * @Param df:
    * @Param mode:
    * @return: void
    */
  def spark_JDBC_mysql(df: DataFrame, mode: SaveMode) = {
    try {
      if (df == null) {
        throw new NullDataframeException("df未赋值,error")
      }
      val connectionProperties = new Properties()
      connectionProperties.load(this.getClass.getClassLoader.getResourceAsStream(JDBC_FIELDS.PROPERTIES_NAME))
      connectionProperties.put(JDBC_FIELDS.USER, connectionProperties.getProperty(JDBC_FIELDS.JDBC_USER))
      connectionProperties.put(JDBC_FIELDS.PASSWD, connectionProperties.getProperty(JDBC_FIELDS.JDBC_PASSWD))
      df.write.mode(mode).jdbc(connectionProperties.getProperty(JDBC_FIELDS.JDBC_URL), connectionProperties.getProperty(JDBC_FIELDS.JDBC_TABLE), connectionProperties)
    }catch {
      case ex: Exception => {
        logger.error(s"写入mysql error:\n${ex.getMessage}")
        ex.printStackTrace()
      }
    }

  }


}
