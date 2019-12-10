package Spark_Utils

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/9
  * Time: 8:59
  *
  * @author zbs
  * @version 1.0
  */
object Spark_Hbase_Utils {
  val load = ConfigFactory.load("hbase.properties")
  private val logger: Logger = Logger.getLogger(this.getClass.getName)
  private val CONNECT_KEY: String = "hbase.zookeeper.quorum"
  private val CONNECT_VALUE: String = load.getString("hbase.zookeeper.host")
  var connection: Connection = null

  /**
    * @Author zbs
    * @Description:创建连接
    * @Date: 2019/12/9
    * @return: void
    */
  def getconf = {
    //1. 获取连接配置对象
    val configuration = HBaseConfiguration.create()
    //2. 设置连接hbase的参数
    configuration.set(CONNECT_KEY, CONNECT_VALUE)
    //3. 获取connection对象
    try {
      connection = ConnectionFactory.createConnection(configuration)
    }
    catch {
      case ex: Exception => {
        logger.error("连接HBase的时候异常！", ex)
      }
    }
    connection
  }


  /**
    * @Author zbs
    * @Description:获得admin
    * @Date: 2019/12/9
    * @Param conf:
    * @return: org.apache.hadoop.hbase.client.Admin
    */

  def getAdmin(conf: Connection): Admin = {
    var admin: Admin = null
    try
      admin = conf.getAdmin
    catch {
      case ex: Exception =>
        logger.error(s"连接HBase的时候异常！, error:${ex.getStackTrace}")
    }
    admin
  }

  /**
    * 关闭admin对象
    *
    * @param admin
    */
  def close(admin: Admin): Unit = {
    if (null != admin)
      try {
        admin.close()
        admin.getConnection.close()
      } catch {
        case e: Exception =>
          logger.error(s"关闭admin的时候异常!${e.getStackTrace}")
      }
  }

  /**
    * 获取表连接
    *
    * @return
    */
  def getTable(conf: Connection, tableName: String): Table = {
    var table: Table = null
    try {
      if (StringUtils.isEmpty(tableName))
        table = conf.getTable(TableName.valueOf(tableName))
    } catch {
      case e: Exception =>
        logger.error(s"获得表对象的时候异常!${e.getStackTrace}")
    }
    table
  }

  /**
    * 关闭admin对象
    *
    * @param table
    */
  def closeTable(table: Table): Unit = {
    if (null != table) try
      table.close()
    catch {
      case e: Exception =>
        logger.error(s"关闭table的时候error!${e.getStackTrace}")
    }
  }
}
