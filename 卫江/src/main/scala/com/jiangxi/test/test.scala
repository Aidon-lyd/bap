package com.jiangxi.test
import scala.collection.immutable

object test {
  def main(args: Array[String]): Unit = {
     var b=List(("aaaa"->1),("bbbb"->1))
     var a=List("bbbb"->1)
     var c=List("aaaa"->1)
     var d=List("cccc"->1)
    val list: List[(String, Int)] = (b++a++c++d).toList
    println(list)
    println(list.groupBy(_._1))
    //Map(bbbb -> List((bbbb,1), (bbbb,1)), cccc -> List((cccc,1)), aaaa -> List((aaaa,1), (aaaa,1)))
    val aaaa: List[(String, Int)] = list.groupBy(_._1).map {
      case (k, a) => {
        (k, a.map(x => {x._2}).sum)
      }
    }.toList
  //List((bbbb,2), (cccc,1), (aaaa,2))
    println(aaaa)
  }
}
