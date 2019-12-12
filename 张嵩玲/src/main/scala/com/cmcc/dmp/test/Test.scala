package com.cmcc.dmp.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/9 16:46 
  * @version 1.0
  */
object Test {
  def main(args: Array[String]): Unit = {
    val res1 = Tuple2("qwe",List(("zs",1),("lisi",1)))
    val res2 = Tuple2("qwe",List(("zs",1),("www",1)))
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(conf)
    val rdd: RDD[(String, List[(String, Int)])] = context.makeRDD(Seq(res1,res2))
    rdd.reduceByKey((list1,list2)=>(list1:::list2)) //(qwe,List((zs,1), (lisi,1), (zs,1), (www,1)))
      .map(x=>{
      val list: List[(String, Int)] = x._2
      val stringToInt: List[(String, Int)] = list.groupBy(_._1)
        .mapValues(_.foldLeft(0)(_ + _._2)).toList

      (x._1,stringToInt)
    })
      .foreach(println)




  }
}
