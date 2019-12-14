package com.jiangxi.test

object First {
  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array[Int](8,5,7,6)
    println(getArr(arr).toBuffer)
  }

  def getArr(arr:Array[Int])={
    var array = Array[Int]()
    val sorted: Array[Int] = arr.sortWith(_ < _)

    val tuples: Array[(Int, Int)] = sorted.map(x => {
      val i: Int = sorted.indexOf(x)
      (x, i)
    })

    for(index <- 0 until  arr.length){
      tuples.foreach{
        case (x,i)=>{
          if (arr(index) == x){
            array :+= i
          }
        }
      }
    }
    array
  }
}
