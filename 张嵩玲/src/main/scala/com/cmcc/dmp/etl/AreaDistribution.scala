package com.cmcc.dmp.etl

import com.cmcc.dmp.Utils.SparkHelper

import com.cmcc.dmp.etl.ProvDataDistribution.getProvDataDistribution
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/6 17:53 
  * @version 1.0
  */
object AreaDistribution {

  def getAreaDistribution(appName: String, master: String, filePath: String): Unit = {
    // 获取sparkConf对象
    val conf: SparkConf = SparkHelper.getSparkConf(appName,master)
    val spark: SparkSession = SparkHelper.getSpark(conf)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 读取数据文件
    val dataFrame: DataFrame = spark.read.parquet(filePath)

    // 分组条件
    val groupCondition: Seq[Column] = Seq[Column](
      $"${ConstantValue.PROVINCENAME}",
      $"${ConstantValue.CITYNAME}")

    // 原始请求数  REQUESTMODE = 1  PROCESSNODE >=1
    val originalReqTotal = LocationField.getOriginalReqTotal(dataFrame)

    // 有效请求数  REQUESTMODE = 1 PROCESSNODE >=2
    val validReqTotal = LocationField.getValidReqTotal(dataFrame)

    // 广告请求数  REQUESTMODE = 1 PROCESSNODE = 3
    val advertisingReqTotal = LocationField.getAdvertisingReqTotal(dataFrame)

    val joinCondition = Seq(ConstantValue.PROVINCENAME,ConstantValue.CITYNAME)
//    originalReqTotal
//      .join(validReqTotal,joinCondition,"left")
//      .join(advertisingReqTotal,joinCondition,"left")
//      .selectExpr(ColumnField.selectReq:_*)
//      .show()
    /*val rdd = dataFrame.rdd
    rdd.map(row=>{
      val provincename = row.getAs[String](ConstantValue.PROVINCENAME)
      val cityname = row.getAs[String](ConstantValue.CITYNAME)
      val requestmode = row.getAs[Int](ConstantValue.REQUESTMODE)
      val processnode = row.getAs[Int](ConstantValue.PROCESSNODE)
      val list: List[Double] = LocationField.requestUtils(requestmode,processnode)
      ((provincename,cityname),list)
    })
        .reduceByKey((list1,list2)=>{
          list1
            .zip(list2)
            .map(x=>{x._1+x._2})
        })*/


    // 参与竞价数  ISEFFECTIVE = '1'  ISBILLING = '1'  ISBID = '1'
    val biddingTotal = LocationField.getBiddingTotal(dataFrame)
    // 竞价成功数  ISEFFECTIVE = '1'  ISBILLING = '1'  ISWIN = '1'  ADORDEERID != 0
    val biddingSuccTotal = LocationField.getBiddingSuccTotal(dataFrame)

//    biddingTotal
//        .join(biddingSuccTotal,joinCondition,"left")
//        .selectExpr(ColumnField.selectBidd:_*)
//        .agg((col(s"${ConstantValue.BIDDINGSUCCTOTAL}")/col(s"${ConstantValue.BIDDINGTOTAL}")).alias("aaa"))
//      .selectExpr(ColumnField.selectBidddouble:_*)
//      .show()





    // 展示数   REQUESTMODE = 2  ISEFFECTIVE = '1'
    // 点击数   REQUESTMODE = 3  ISEFFECTIVE = '1'
    // DSP广告消费  ISEFFECTIVE = '1'  ISBILLING = '1'  ISWIN = '1'
    // DSP广告成本  ISEFFECTIVE = '1'  ISBILLING = '1'  ISWIN = '1'



    val res = dataFrame
        .groupBy(groupCondition: _*)
        .agg(
          expr("SUM(CASE WHEN requestmode=1 and processnode>=1 THEN 1 ELSE 0 END)").alias(ConstantValue.ORIGINALREQTOTAL),
          expr("SUM(CASE WHEN requestmode=1 and processnode>=2 THEN 1 ELSE 0 END)").alias(ConstantValue.VALIDREQTOTAL),
          expr("SUM(CASE WHEN requestmode=1 and processnode=3 THEN 1 ELSE 0 END)").alias(ConstantValue.ADVERTISINGREQTOTAL),
          expr("SUM(CASE WHEN iseffective='1' and isbilling=1 and isbid=1 THEN 1 ELSE 0 END)").alias(ConstantValue.BIDDINGTOTAL),
          expr("SUM(CASE WHEN iseffective='1' and isbilling=1 and iswin=1 and adorderid != 0 THEN 1 ELSE 0 END)").alias(ConstantValue.BIDDINGSUCCTOTAL),
          expr("SUM(CASE WHEN requestmode=2 and iseffective=1 THEN 1 ELSE 0 END)").alias(ConstantValue.SHOWTOTAL),
          expr("SUM(CASE WHEN requestmode=3 and iseffective=1 THEN 1 ELSE 0 END)").alias(ConstantValue.CLICKTOTAL),
          expr("SUM(CASE WHEN iseffective=1 and isbilling=1 and iswin=1 THEN WinPrice ELSE 0 END)/1000").alias(ConstantValue.DSPADVERTISINGEXP),
          expr("SUM(CASE WHEN iseffective=1 and isbilling=1 and iswin=1 THEN adpayment ELSE 0 END)/1000").alias(ConstantValue.SPADVERTISINGCost)

        )
      .select(
        col(s"${ConstantValue.ORIGINALREQTOTAL}"),
        col(s"${ConstantValue.VALIDREQTOTAL}"),
        col(s"${ConstantValue.ADVERTISINGREQTOTAL}"),
        col(s"${ConstantValue.BIDDINGTOTAL}"),
        col(s"${ConstantValue.BIDDINGSUCCTOTAL}"),
        (col(s"${ConstantValue.BIDDINGSUCCTOTAL}").cast(DoubleType) / col(s"${ConstantValue.BIDDINGSUCCTOTAL}").cast(DoubleType)).alias(ConstantValue.BIDDINGSUCCRATE),
        col(s"${ConstantValue.SHOWTOTAL}"),
        col(s"${ConstantValue.CLICKTOTAL}"),
        (col(s"${ConstantValue.CLICKTOTAL}").cast(DoubleType) / col(s"${ConstantValue.SHOWTOTAL}").cast(DoubleType)).alias(ConstantValue.CLICKRATE),
        col(s"${ConstantValue.DSPADVERTISINGEXP}"),
        col(s"${ConstantValue.SPADVERTISINGCost}")
      )


    res.show()





    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val master = "local[2]"
    val filePath = "out/*.parquet"
    getAreaDistribution(appName,master,filePath)
  }
}
