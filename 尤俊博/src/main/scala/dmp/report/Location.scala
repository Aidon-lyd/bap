package dmp.report

import dmp.util.{ColumnsHelper, ConfigHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Location {
  def getLocation(spark: SparkSession): Unit ={
    //定义参数
    val sourceDataFile: String = ConfigHelper.adFormatData  //输入路径

    import spark.implicits._
    import  org.apache.spark.sql.functions._
    val data: DataFrame = spark.read.parquet(sourceDataFile).cache()

    val provinceFinalData: DataFrame = data.groupBy("provincename")
      .agg(expr("COUNT(1)"), ColumnsHelper.getLocationProColumns(): _*)
      .selectExpr(ColumnsHelper.getLocationFinalColumns("provincename"): _*)

    val cityFianlData: DataFrame = data.groupBy("provincename","cityname")
      .agg(expr("COUNT(1)"), ColumnsHelper.getLocationProColumns(): _*)
      .selectExpr(ColumnsHelper.getLocationFinalColumns("provincename","cityname"): _*)

    provinceFinalData.show(100,false)
    cityFianlData.show(100,false)

    spark.stop()

  }
}
