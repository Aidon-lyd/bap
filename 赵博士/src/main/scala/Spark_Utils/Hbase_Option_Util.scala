package Spark_Utils

import Spark_Utils.Spark_Hbase_Utils.logger
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.{Admin, Durability}
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.slf4j.LoggerFactory

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/9
  * Time: 20:06
  *
  * @author zbs
  * @version 1.0
  */
object Hbase_Option_Util {
  /**
    * @Author zbs
    * @Description:创建表
    * @Date: 2019/12/9
    * @Param admin:
    * @Param tableName:
    * @Param hColumn:
    * @return: void
    */
  def createTbale(admin: Admin, tableName: String, hColumn: String) = {
    val logger=LoggerFactory.getLogger(this.getClass)
    try {
      val BLOOM_TYPE = BloomType.ROW
      val WAL = Durability.ASYNC_WAL
      val load: Config = Spark_Hbase_Utils.load
      //
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      //2. 创建列簇表述qi
      val columnDescriptor = new HColumnDescriptor(hColumn)
      columnDescriptor.setTimeToLive(load.getInt("columnDescriptor.setTimeToLive").toInt) // 秒为单位
      columnDescriptor.setMinVersions(load.getInt("columnDescriptor.setMinVersions"))
      columnDescriptor.setMaxVersions(load.getInt("columnDescriptor.setMaxVersions"))
      columnDescriptor.setBloomFilterType(BLOOM_TYPE)
      columnDescriptor.setDFSReplication(load.getInt("columnDescriptor.setDFSReplication").toShort) // 设置HBase数据存放的副本数
      columnDescriptor.setBlockCacheEnabled(load.getBoolean("columnDescriptor.setBlockCacheEnabled"))
      //2.2 将列簇添加到表中
      tableDescriptor.addFamily(columnDescriptor)
      //设置wal的机制
      tableDescriptor.setDurability(WAL)
      admin.createTable(tableDescriptor); //创建与分区表
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("连接HBase的时候异常！", ex.getStackTrace)
      }
    }
  }
}
