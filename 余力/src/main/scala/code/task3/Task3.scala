package code.task3

import org.apache.spark.sql.functions._
import bean.CaseClass.LogData
import code.task1.Task1
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import code.task3.Task3Helper._

object Task3 {
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
    import spark.implicits._

    //5.Get logData
    val logData: DataFrame = spark.read.parquet("data/parquetData/*")

    //6.Cache logData
    logData.cache()

    //7.get provinceFinalData
    val provinceFinalData: DataFrame = logData.groupBy("provincename")
      .agg(expr("COUNT(1)"), getColumnFormatOne(): _*)
      .selectExpr(getColumnFormatTwo("provincename"): _*)

    //8.get cityFinalData
    val cityFinalData: DataFrame = logData.groupBy("cityname")
      .agg(expr("COUNT(1)"), getColumnFormatOne(): _*)
      .selectExpr(getColumnFormatTwo("cityname"): _*)

    //9.Get finalData
    val finalData: DataFrame = provinceFinalData.union(cityFinalData)
      .withColumnRenamed("provincename", "provinceOrCityName")

    //10.Show finalData
    finalData.show(100, false)

    //11.Stop Spark Session
    spark.stop()
  }
}
