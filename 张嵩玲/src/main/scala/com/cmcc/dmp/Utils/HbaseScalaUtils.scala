package com.cmcc.dmp.Utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * requirement  从聚合表获取昨天的数据
  *
  * @author zhangsl
  * @date 2019/12/13 14:23 
  * @version 1.0
  */
object HbaseScalaUtils {
  def getYesterdayData(context: SparkContext,totalLabeltableName:String,totalLabelcolumnDescriptor:String,day: String):RDD[(String, List[(String, Double)])]={
    // 从hbase读取数据并格式化
    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181");
    configuration.set(TableInputFormat.INPUT_TABLE,totalLabeltableName)
    val HbaseRDD: RDD[(ImmutableBytesWritable, Result)] =
      context.newAPIHadoopRDD(configuration,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    HbaseRDD.map(r => {
      val rowKey: String = Bytes.toString(r._2.getRow())
      val label: String = Bytes.toString(r._2.getValue(Bytes.toBytes(totalLabelcolumnDescriptor), Bytes.toBytes(DateUtils.getTimeAgo(day))))
      (rowKey, label)
    })
      .map(x => {
        var list = List[(String, Double)]()
        val strings: Array[String] = x._2.split(",")
        for (s <- strings) {
          val label: String = s.split(":")(0)
          val count: Double = s.split(":")(1).toDouble
          list :+= (label, count)
        }
        (x._1, list)
      })
  }
}
