package code.task4

import bean.CaseClass.LogData
import code.task1.Task1
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import code.task4.Task4Helper._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import util.UDFUtil._
import util.UDAFUtil._
import org.apache.spark.sql.functions._
import java.util

import org.apache.hadoop.hbase.util.Bytes
import org.json4s.DefaultFormats
import org.json4s.native.Json

import scala.util.parsing.json.JSON

object Task4 {
  def main(args: Array[String]): Unit = {
    //1.Get Configuration
    val conf: SparkConf = new SparkConf()
    conf.setAppName(Task1.getClass.getName).setMaster("local[8]")

    //2.Register for the Kryo
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired ", "true")
    //The following may need to add ,classOf[mergeCommonTag.type]
    conf.registerKryoClasses(Array(classOf[LogData]))

    //3.Get Spark Session
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //4.Import
    import spark.implicits._

    //5.Register UDF and UDAF
    //5.1 Register UDF
    spark.udf.register("isValidKeywords", isValidKeyword(_: String))
    spark.udf.register("getKeywordsTag", getKeywordsTag(_: String))
    spark.udf.register("isValidLongAndLat", isValidLongAndLat(_: String, _: String))
    spark.udf.register("getTradeMarkTags", getTradeMarkTags(_: String, _: String))
    //5.2 Register UDAF
    spark.udf.register("mergeCommonTag", mergeCommonTag)
    spark.udf.register("mergeSpecialTag", mergeSpecialTag)

    //6.Get logData
    val logData: DataFrame = spark.read.parquet("data/parquetData/*")

    //7.Get logDataWithTags
    val logDataWithTags: DataFrame = logData.selectExpr(getColumnFormatOne: _*)

    //8.Get mergedTagsData
    //May need to rename the mergedTagsData
    val mergedTagsData: DataFrame = logDataWithTags.groupBy("uuid")
      .agg(expr("COUNT(1)"), getColumnFormatTwo: _*)

    //9.Write the mergedTagsData to Hbase
    mergedTagsData.foreachPartition(rowsPerPar => {
      val configuration: Configuration = HBaseConfiguration.create

      configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181")

      val connection = ConnectionFactory.createConnection(configuration)

      val admin = connection.getAdmin

      val table: Table = connection.getTable(TableName.valueOf("SparkProjectTwoTags"));

      var put: Put = null
      val puts: util.List[Put] = new util.ArrayList[Put]

      rowsPerPar.foreach(row => {
        var allMap = Map[String, Any]()

        //Begin From 2 Because Of COUNT(1)
        for (i <- 2 to row.size - 1) {
          val noTransformedJson: Option[Any] = JSON.parseFull(row.getString(i))

          val transformedJson: Map[String, Any] = transformCommonJson(noTransformedJson)

          allMap ++= transformedJson
        }

        val allJson: String = Json(DefaultFormats).write(allMap)

        put = new Put(Bytes.toBytes(row.getString(0)))

        put.addColumn(Bytes.toBytes("tags"), Bytes.toBytes("tagsPerPerson"), Bytes.toBytes(allJson))

        puts.add(put)
      })

      table.put(puts)

      //The Following Code May need to be Optimized By using Try Catch
      if (table != null)
        table.close()

      if (connection != null)
        connection.close()
    })

    //10.Stop Spark Seesion
    spark.stop()
  }
}
