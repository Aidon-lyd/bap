package code.task1

import bean.CaseClass.LogData
import bean.CaseClassHelper.getLogDataFromRow
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object Task1 {
  def main(args: Array[String]): Unit = {
    //1.Get Configuration
    val conf: SparkConf = new SparkConf()
    conf.setAppName(Task1.getClass.getName).setMaster("local[8]")

    //2.Register for the Kryo
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrationRequired ","true")
    conf.registerKryoClasses(Array(classOf[LogData]))

    //3.Get Spark Session
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //4.Import
    import spark.implicits._

    //5.Get logData
    //Can be Optimized By using CSV With Schema
    val logData: Dataset[LogData] = spark.read.text("data/sourceData/logData/2016-10-01_06_p1_invalid.1475274123982.log.FINISH")
      .filter(_.getString(0).split(",",-1).size == 85)
      .map(row => getLogDataFromRow(row.getString(0).split(",",-1)))

    //6.Write the logData with Kryo Serialization and Parquet Storage Format
    logData.write.parquet("data/parquetData")

    //7.Stop Spark Session
    spark.stop()
  }

}
