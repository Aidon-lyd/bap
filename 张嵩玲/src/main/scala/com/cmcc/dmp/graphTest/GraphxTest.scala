package com.cmcc.dmp.graphTest

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/11 8:53 
  * @version 1.0
  */
object GraphxTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("graph").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 图计算的要求是想要构建图的话，必须要有点集合和边集合
    // 构建点集合
    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("小明", 50)),
      (2L, ("小红", 40)),
      (6L, ("小丁", 27)),
      (9L, ("小苍", 35)),
      (133L, ("小刚", 30)),
      (138L, ("小赵", 23)),
      (16L, ("小刘", 30)),
      (44L, ("小李", 35)),
      (21L, ("小迪", 25)),
      (5L, ("小王", 29)),
      (7L, ("小陈", 23)),
      (158L, ("小张", 26))
    ))
    //  边的集合
    val edge: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L,0),
      Edge(2L, 133L,0),
      Edge(6L, 133L,0),
      Edge(9L, 133L,0),
      Edge(6L, 138L,0),
      Edge(16L, 138L,0),
      Edge(44L, 138L,0),
      Edge(21L, 138L,0),
      Edge(5L, 158L,0),
      Edge(7L, 158L,0)
    ))
    // 构建图
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD,edge)
    // 取顶点ID
    val comm = graph.connectedComponents().vertices
    println(comm.collect().toBuffer)
    comm.join(vertexRDD).map{
      case (userid,(cmId,(name,age))) =>(cmId,List(name,age))
    }
      .reduceByKey(_++_)
      .foreach(println)


  }
}
