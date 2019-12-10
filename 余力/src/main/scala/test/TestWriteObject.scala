package test

import bean.CaseClass.LogData
import code.task1.Task1
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestWriteObject {
  def main(args: Array[String]): Unit = {
    //1.Get Configuration
    val conf: SparkConf = new SparkConf()
    conf.setAppName(Task1.getClass.getName).setMaster("local[8]")

    //2.Register for the Kryo
    //May have Problem
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired ","true")
    conf.registerKryoClasses(Array(classOf[LogData]))

    //3.Get Spark Session
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //4.Import
    import spark.implicits._

    //5.Test
    spark.read.parquet("data/parquetData/part-00000-19bd9a52-d1fc-469b-8822-3d6e900489e8-c000.snappy.parquet").as[LogData].show(100,false)

    //6.stop Spark Session
    spark.stop()
  }
}
