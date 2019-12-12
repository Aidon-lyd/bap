package com.cmcc.dmp.Utils

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/6 16:13 
  * @version 1.0
  */
object NumberUtils {
  // int类型为空赋值0
  def intEmpty(a:String):Int={
    if (a.isEmpty) {
      return 0
    }else{
      return a.trim.toInt
    }
  }
  // double l类型为空赋值0.0
  def doubleEmpty(a:String):Double={
    if (a.isEmpty) {
      return 0.0
    }else{
      return a.trim.toDouble
    }
  }
}
