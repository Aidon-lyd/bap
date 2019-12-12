package com.cmcc.dmp.test

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * requirement   好友推荐
  *
  * @author zhangsl
  * @date 2019/12/11 10:46 
  * @version 1.0
  */
object CommonFriends {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val context = new SparkContext(conf)

    // 读取数据
    val data = context.textFile("bak/friends.txt")//.map(_.split(" "))
    //data.foreach(println)

    //构建点集合RDD[(long,String)]   (21416623,刘闻甲)
    val pointListRDD: RDD[(Long, String)] = data
      .flatMap(_.split(" "))
      .map(data => {
        val nameLong = data.hashCode.toLong
        (nameLong, data)
      })
    pointListRDD.foreach(println)


    // 构建边集合 RDD[Edge[ED]]  Edge(21416623,24061958,0)
    val edgeRDD: RDD[Edge[Int]] =
      data
      .map(_.split(" "))
      .flatMap(arr => {
        val headNameLong = arr.head.hashCode.toLong
        arr.map(name => Edge(headNameLong, name.hashCode.toLong, 0))
      })
    //edgeRDD.foreach(println)

    // 构建图对象
    val graph: Graph[String, Int] = Graph(pointListRDD,edgeRDD)

    // 调用API
    // 取到图形的顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    vertices.join(pointListRDD)
        .map{
          case (userID,(commonId,name))=>(commonId,Set(name)) // 聚合时去重
        }
      .reduceByKey(_++_)
      .map(_._2.mkString(","))
        .foreach(println)

    // 刘闻甲,王雷凯,张俊杰,慧慧,李海龙,张阳
    // 龙龙


    context.stop()
  }
}
