package ADS_Tag

import Spark_Utils.{Hbase_Option_Util, SparkSession_Utils, Spark_Hbase_Utils}
import com.typesafe.config.Config
import ods_release.ODS_Relase
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, Connection, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/9
  * Time: 15:23
  *
  * @author zbs
  * @version 1.0
  */
object User_Tag {
  private val LEVEL = StorageLevel.MEMORY_AND_DISK
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  private val writable = new ImmutableBytesWritable()
  private var put: Put = null
  private val Family = "tags"
  private val CLOUMN = "2019-12-09"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getName)
    val spark: SparkSession = SparkSession_Utils.getSpark(conf)
    User_Tag_Dispose(spark)
    val sc = spark.sparkContext
    val configuration = sc.hadoopConfiguration
  }

  /**
    * @Author zbs
    * @Description:构建数据标签
    * @Date: 2019/12/7
    * @Param spark:
    * @return: void
    */
  def User_Tag_Dispose(spark: SparkSession) = {
    import spark.implicits._
    try {
      val ODS_df: DataFrame = ODS_Relase.ods_Dispose(spark).persist(LEVEL)
      //    ODS_df.selectExpr("uuid").show(100)
      val data_Tag = ODS_df.filter(_.getString(15).nonEmpty).map(
        prelin => {
          val uuid: String = prelin.getString(15)
          val list1: List[(String, Int)] = Ad_Tag.AD_Tag_Dispose(prelin.toSeq)
          val list2: List[(String, Int)] = KeyWord_Tag.makeTags(prelin.toSeq)
          val list3: List[(String, Int)] = Business_Tag.makeTag(prelin.toSeq)
          (uuid, list1++list2++list3)
        }
      )
      val res_rdd: RDD[(String, List[(String, Int)])] = data_Tag.rdd.reduceByKey(
        (list1, list2) => {
          (list1 ::: list2)
            .groupBy(_._1 + "")
            .mapValues(_.foldLeft(0)(_ + _._2))
            .toList
        })
      val res_value: RDD[(ImmutableBytesWritable, Put)] = res_rdd.map {
        case (userid, userTags) => {
          val put = new Put(Bytes.toBytes(userid))
          //        (userid,"appname:1,ad23:2,")
          val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
          // 对应的rowKey下面的列簇  列名  列的value值
          put.addImmutable(Bytes.toBytes(Family), Bytes.toBytes(CLOUMN), Bytes.toBytes(tags))
          // 存入Hbase
          (writable, put)
        }
      }


      // Tag_Aftle_Data.saveAsHadoopDataset()
      val conf = Spark_Hbase_Utils.getconf
      save_hbase(conf, res_value)


    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error(s"构建数据标签,error:\n${ex.getStackTrace}")
      }
    }
  }

  /**
    * @Author zbs
    * @Description:将数据保存到hbase
    * @Date: 2019/12/9
    * @Param conf:
    * @return: void
    */
  def save_hbase(conf: Connection, rdd: RDD[(ImmutableBytesWritable, Put)]) = {
    try {
      val admin: Admin = Spark_Hbase_Utils.getAdmin(conf)
      val load: Config = Spark_Hbase_Utils.load
      val TABLE_NAME: String = load.getString("hbase.table.Name")
      val jobConf = new JobConf(conf.getConfiguration)
      // 指定Key输出的类型
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      // 是定输出到哪张表
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME)
      if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
        Hbase_Option_Util.createTbale(admin, TABLE_NAME, Family)
      }

      rdd.saveAsHadoopDataset(jobConf)
      Spark_Hbase_Utils.close(admin)
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error(s"保存到hbase时,error:\n${ex.getStackTrace}")
      }
    }
  }

}
