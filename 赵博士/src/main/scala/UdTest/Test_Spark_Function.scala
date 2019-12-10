package UdTest


import Spark_Utils.SparkSession_Utils
import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  * User: zbs
  * Date: 2019/12/6
  * Time: 14:12
  *
  * @author zbs
  * @version 1.0
  */

object Test_Spark_Function {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession=null
    SparkSession_Utils.register(spark)
  }
}
