package com.jiangxi.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
object rdd2hbase {
  def data2hbase(rdd:RDD[(String, List[(String, Int)])], tablename:String){
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","node245,node246,node247")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    //初始化jobconf
    val jobConf = new JobConf(conf)
    jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Put])
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    /*一个Put对象就是一行记录，在构造方法中指定主键
     * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
     * Put.add方法接收三个参数：列族，列名，数据
     */
 /*   rdd.foreachPartition(it=>{
      it.map(elem => {
        val userid = elem._1
        val tags = elem._2
        val put :Put = new Put(Bytes.toBytes(userid.toString)) //在这里指定了主键
        val resTags = tags.map(item => item._1 + ":" + item._2).mkString(",")
        put.addColumn(Bytes.toBytes("user_tags"), Bytes.toBytes("day1"), Bytes.toBytes(resTags))
        put
      })
      //连接hbase，写入一个分区的数据
        val tableName: TableName = TableName.valueOf(tablename)
        val conn: Connection = HbaseUtil.getConnection()
        val table: Table = conn.getTable(tableName)
        table.put()
    })*/


  }
}
