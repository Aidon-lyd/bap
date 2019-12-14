package dmp.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

object Save2Hbase {
  def saveToHbase(rdd: RDD[(String, String)]) = {
    val zookeeperQuorum = "node245:2181"
    val hdfsRootPath = "hdfs://node245:9000/"
    val hFilePath = "hdfs://node245:9000/hbase/bulkload/hfile/"
    val tableName = "Tags"
    val familyName = "Day01"
    val qualifierName = "title"

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", hdfsRootPath)
    val fileSystem = FileSystem.get(hadoopConf)
    val hbaseConf = HBaseConfiguration.create(hadoopConf)
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin

    //表不存在就建表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      val hcd = new HColumnDescriptor(familyName)
      desc.addFamily(hcd)
      admin.createTable(desc)
    }

    // 如果存放 HFile文件的路径已经存在，就删除掉
    if(fileSystem.exists(new Path(hFilePath))) {
      fileSystem.delete(new Path(hFilePath), true)
    }

    //处理数据
    val data: RDD[(ImmutableBytesWritable, KeyValue)] = rdd.sortByKey(true).map(tuple => {
      val kv = new KeyValue(Bytes.toBytes(tuple._1), Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), Bytes.toBytes(tuple._2))
      (new ImmutableBytesWritable(Bytes.toBytes(tuple._1)), kv)
    })

    // 2. Save Hfiles on HDFS
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)

    job.getConfiguration.set("mapred.output.dir", hFilePath)
    data.saveAsNewAPIHadoopDataset(job.getConfiguration)

    //  3. Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    val regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName))
    bulkLoader.doBulkLoad(new Path(hFilePath), admin, table, regionLocator)

    hbaseConn.close()
    fileSystem.close()
  }
}
