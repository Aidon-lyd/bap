package dmp.etl

import dmp.bean.Log
import dmp.traits.Processor
import dmp.util.{ColumnsHelper, ConfigHelper}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}


object ETLProcessor extends Processor{
  override def processor(spark: SparkSession): Unit = {
    //定义参数
    val sourceDataFile: String = ConfigHelper.adDataPath

    import spark.implicits._
    //读数据
   val data: Dataset[String] = spark.read.textFile(sourceDataFile)
//    //读数据更好的方式
    val datacsv: DataFrame = spark.read.schema(ColumnsHelper.getStructType()).csv(sourceDataFile)
    datacsv.coalesce(1).write.parquet("data/parquet")

    /*
    val data_formatted: DataFrame  = data.mapPartitions(item => {
      item.map(row => row.split(",", -1))
        .filter(arr => arr.length == 85)
        .map(line  =>
        Log(
          line(0),
          line(1).toInt,
          line(2).toInt,
          line(3).toInt,
          line(4).toInt,
          line(5),
          line(6),
          line(7).toInt,
          line(8).toInt,
          line(9).toDouble,
          line(10).toDouble,
          line(11),
          line(12),
          line(13),
          line(14),
          line(15),
          line(16),
          line(17).toInt,
          line(18),
          line(19),
          if (line(20).isEmpty) -1 else line(20).toInt,
          if (line(21).isEmpty) -1 else line(21).toInt,
          line(22),
          line(23),
          line(24),
          line(25),
          line(26).toInt,
          line(27),
          line(28).toInt,
          line(29),
          line(30).toInt,
          line(31).toInt,
          line(32).toInt,
          line(33),
          line(34).toInt,
          line(35).toInt,
          line(36).toInt,
          line(37),
          line(38).toInt,
          line(39).toInt,
          line(40).toDouble,
          line(41).toDouble,
          line(42).toInt,
          line(43),
          line(44).toDouble,
          line(45).toDouble,
          line(46),
          line(47),
          line(48),
          line(49),
          line(50),
          line(51),
          line(52),
          line(53),
          line(54),
          line(55),
          line(56),
          line(57).toInt,
          line(58).toDouble,
          line(59).toInt,
          line(60).toInt,
          line(61),
          line(62),
          line(63),
          line(64),
          line(65),
          line(66),
          line(67),
          line(68),
          line(69),
          line(70),
          line(71),
          line(72),
          if (line(73).isEmpty) -1 else line(20).toInt,
          line(74).toDouble,
          line(75).toDouble,
          line(76).toDouble,
          line(77).toDouble,
          line(78).toDouble,
          line(79),
          line(80),
          line(81),
          line(82),
          line(83),
          if (line(84).isEmpty) -1 else line(20).toInt)
        )
    }).toDF().cache()

     */

//    data_formatted.collect().foreach(println)
//    data_formatted.coalesce(1).write.parquet("data/parquet")
  }


}
