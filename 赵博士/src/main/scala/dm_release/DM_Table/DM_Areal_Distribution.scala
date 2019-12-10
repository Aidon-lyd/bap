package dm_release.DM_Table

import Exceptions.SparkSessionInitException
import Spark_Utils.SparkSession_Utils
import dm_release.DM_Fields.DM_Area_Fields
import ods_release.ODS_Relase
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/7
  * Time: 14:16
  *
  * @author zbs
  * @version 1.0
  */
object DM_Areal_Distribution {
  val LEVEL = StorageLevel.MEMORY_AND_DISK
  val TEXT_File_Type = "text"
  val PARQUET_File_Type = "parquet"
  val JSON_File_Type = "json"
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
/**
  * @Author zbs
  * @Description:构建地域分布指标
  * @Date: 2019/12/7
  * @Param spark:
  * @return: void
  */
  def Areal_DistriBution_Bulid(spark: SparkSession) = {
    try {
      if(spark==null){
        throw new SparkSessionInitException("spark初始化，error")
      }
      val ods_df: DataFrame = ODS_Relase.ods_Dispose(spark)
      ods_df
        .selectExpr(DM_Area_Fields.condition_Fields:_*)
        .groupBy(DM_Area_Fields.GROUP_FIEDS_PRO,DM_Area_Fields.GROUP_FIEDS_CITY)
        .agg(
          sum(DM_Area_Fields.PART_BIDDING),
          sum(DM_Area_Fields.SUCC_BIDDINGG),
          sum(DM_Area_Fields.DISPLAY),
          sum(DM_Area_Fields.CLICK),
          sum(DM_Area_Fields.AD_CONSUMED)/1000,
          sum(DM_Area_Fields.AD_COST)/1000
        )
        .repartition(5)
        .persist(LEVEL)
        .show(100)
    }
    catch {
      case exception: Exception => {
        exception.printStackTrace()
        logger.error(s"DM层地域划分时，error:${exception.getMessage}")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getName)
    val spark: SparkSession = SparkSession_Utils.getSpark(conf)
    Areal_DistriBution_Bulid(spark)
  }
}
