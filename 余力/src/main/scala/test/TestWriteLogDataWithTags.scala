package test

import bean.CaseClass.LogData
import code.task1.Task1
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestWriteLogDataWithTags {
  def main(args: Array[String]): Unit = {
    //1.Get Configuration
    val conf: SparkConf = new SparkConf()
    conf.setAppName(Task1.getClass.getName).setMaster("local[8]")

    //2.Get Spark Session
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //3.Import
    import spark.implicits._

    //4.Test
    spark.read.parquet("data/logWithTagsData/*").show(100,false)

    //5.stop Spark Session
    spark.stop()
  }
}
