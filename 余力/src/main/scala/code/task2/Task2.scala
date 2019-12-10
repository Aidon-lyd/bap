package code.task2

import bean.CaseClass.LogData
import code.task1.Task1
import constant.Constant._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import util.PropertiesUtil.getPropertiesOfJDBCMySQL

object Task2 {
  def main(args: Array[String]): Unit = {
    //1.Get Configuration
    val conf: SparkConf = new SparkConf()
    conf.setAppName(Task1.getClass.getName).setMaster("local[8]")

    //2.Register for the Kryo
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired ", "true")
    conf.registerKryoClasses(Array(classOf[LogData]))

    //3.Get Spark Session
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //4.Import

    //5.Get logData
    val logData: DataFrame = spark.read.parquet("data/parquetData/*")

    //6.Transform and Get finalData
    val finalData: DataFrame = logData.groupBy("provincename", "cityname")
      .count()
      .withColumnRenamed("count", "ct")

    //8.Cache the finalData
    finalData.cache()

    //7.Write the finalData As Json Storage Format
    finalData.coalesce(1).write.json("data/jsonData")

    //8.Write the finalData to MySQL
    //Maybe it can Optimize with Transform DataFrame to DataSet[Case Class]
    finalData.write.mode(SaveMode.Overwrite).jdbc(MYSQL_JDBC_URL_NO_DB+DB_IN_MYSQL_JDBC_URL,COUNT_DISTRIBUTE_PER_PROVINCE_CITY_ADS_TABLE,getPropertiesOfJDBCMySQL())

    //9.Stop Spark Session
    spark.stop()
  }
}
