package code.task5

import java.net.InetAddress

import code.task1.Task1
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import code.task5.Task5Helper._
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.json4s.DefaultFormats
import org.json4s.native.Json

object Task5 {
  def main(args: Array[String]): Unit = {
    //YL:Note 1.Get Configuration
    val conf: SparkConf = new SparkConf()
    conf.setAppName(Task1.getClass.getName).setMaster("local[8]")

    //YL:Note 2.Get Spark Session
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //YL:Note 3.Import
    import spark.implicits._

    //YL:Note 4.Get logDataWithTags
    val logDataWithTags: DataFrame = spark.read.parquet("data/logWithTagsData/*.parquet")

    //YL:Note 5.Get transformedLogDataWithTags
    val transformedLogDataWithTags: Dataset[Row] = logDataWithTags.select(getColumnFormatOne(): _*)
      .select(getColumnFormatTwo: _*)
      //YL:ToBeOptimized Optimized By filter in the process of the data cleaning
      .filter("idMappingSourceID != ''")
    transformedLogDataWithTags.cache()

    //YL:Note 6.Get vertexRDD
    //YL:ToBeOptimized May Be Optimized By using UDF
    //YL:PossibleError getColumnFormatOne and getColumnFormatTwo May Have Problems

    val vertexRDD: RDD[(Long, String)] = transformedLogDataWithTags.rdd
      .mapPartitions(rowsPerPar => {
        rowsPerPar.map(row => {
          if (row.getInt(1) == 1) {
            var tags: String = ""

            for (i <- 3 to 12)
              tags += row.getString(i) + "|"

            (row.getString(2).hashCode.toLong, tags)
          }
          else {
            (row.getString(2).hashCode.toLong, "")
          }
        })
      })

    //YL:Note 6.Get edgeRDD

    val leftTable: Dataset[Row] = transformedLogDataWithTags
      .withColumnRenamed("rowNumber", "lRowNumber")
      .withColumnRenamed("idMappingSourceNumber", "lIdMappingSourceNumber")
      .withColumnRenamed("idMappingSourceID", "lIdMappingSourceID")

    val rightTable: Dataset[Row] = transformedLogDataWithTags
      .withColumnRenamed("rowNumber", "rRowNumber")
      .withColumnRenamed("idMappingSourceNumber", "rIdMappingSourceNumber")
      .withColumnRenamed("idMappingSourceID", "rIdMappingSourceID")

    val edgeRDD: RDD[Edge[Int]] = leftTable.join(rightTable, expr("lRowNumber = rRowNumber"))
      //YL:ToBeOptimized Can be Optimized By Soft Code
      //YL:ToBeOptimized Can be Optimized By Filtering the Reverse Edge
      .filter("lIdMappingSourceNumber != rIdMappingSourceNumber")
      .select(expr("lRowNumber"), expr("lIdMappingSourceNumber"), expr("lIdMappingSourceID"), expr("rIdMappingSourceNumber"), expr("rIdMappingSourceID"))
      .rdd
      .mapPartitions(rowPerPar => {
        rowPerPar.map(row => {
          Edge(row.getString(2).hashCode.toLong, row.getString(4).hashCode.toLong, 0)
        })
      })

    //YL:Note 7.Get MergedTagsPerPerson and Save to the Elasticsearch
    //YL:ToBeOptimized Can be Optimized By Using String Not LONG AS Vertex ID
    val graph: Graph[String, Int] = Graph(vertexRDD, edgeRDD)

    val connectedComponentsVertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    connectedComponentsVertices.join(vertexRDD).mapPartitions(rowsPerPar =>
      rowsPerPar.map({
        //YL:PossibleError Without Case _ Or Case Any Or Something Else May Lead to Problems
        case (specialId, (commonId, tags)) => (commonId, tags)
      }))
      .reduceByKey((firstValue, secondValue) => {
        firstValue ++ secondValue
      })
      .foreachPartition(rowPerPar => {
        //YL:ToBeOptimized Can be Optimized By Soft Code
        val settings: Settings = Settings.builder.put("cluster.name", "yuli").build
        val transportClient = new PreBuiltTransportClient(settings)

        transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop01"), 9300))

        rowPerPar.foreach(row => {
          var mergedMap: Map[String, VertexId] = Map()

          row._2.split("\\|").filter(_ != "").foreach(tag => {
            if (mergedMap.contains(tag))
              mergedMap.+=((tag, mergedMap.get(tag).get + 1))
            else
              mergedMap.+=((tag, 1L))
          })

          var jsonMap: Map[String, String] = Map()

          //YL:ToBeOptimized Can be Optimized By using JsonString
          jsonMap.+=(("value", mergedMap.toString()))

          val jsonString: String = Json(DefaultFormats).write(jsonMap)

          //YL:ToBeOptimized Can be Optimized By using Batch Processing
          transportClient.prepareIndex("spark_product_two", "Tags", row._1.toString).setSource(jsonString, XContentType.JSON).get()
        })

        //YL:ToBeSolved Need to Solve the Two Netty JAR Clash Of Different Version
        if (transportClient != null)
          transportClient.close()
      })

    //YL:Note 9.Stop Spark Seesion
    spark.stop()
  }
}
