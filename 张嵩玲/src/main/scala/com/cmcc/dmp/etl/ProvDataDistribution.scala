package com.cmcc.dmp.etl

import com.cmcc.dmp.Utils.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/6 16:42 
  * @version 1.0
  */
object ProvDataDistribution {

  def getProvDataDistribution(appName: String, master: String, filePath:String,outFilePath:String): Unit = {
    // 获取sparkConf对象
    val conf: SparkConf = SparkHelper.getSparkConf(appName,master)
    val spark: SparkSession = SparkHelper.getSpark(conf)

    import spark.implicits._
    import org.apache.spark.sql.functions._


    // 读取数据文件
    val dataFrame: DataFrame = spark.read.parquet(filePath)
    val groupCondition: Seq[Column] = Seq[Column](
      $"${ConstantValue.PROVINCENAME}",
      $"${ConstantValue.CITYNAME}")
    val provDataDistributionRes: DataFrame = dataFrame
      .coalesce(1)
      .groupBy(groupCondition:_*)
      .agg(count(s"${ConstantValue.SESSIONID}").alias(ConstantValue.CT))
      .selectExpr(ColumnField.selectprovDataDistribution:_*)
      .persist(ConstantValue.STORAGELEVEL)


    provDataDistributionRes.show(100)

    // 保存到磁盘 json格式
    provDataDistributionRes.write.json(outFilePath)

    // 保存到mysql
    provDataDistributionRes.write.format("jdbc").mode(SaveMode.Overwrite)
        .option("url","jdbc:mysql://localhost:3306/spark_streaming")
        .option("user","root")
        .option("password","123456")
        .option("dbtable","tb_provdistribute")
        .save()

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val master = "local[2]"
    val filePath = "out/*.parquet"
    val outFilePath = "outJSON"
    getProvDataDistribution(appName,master,filePath,outFilePath)
  }
}
