package com.cmcc.dmp.Utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat


/**
  * requirement  时间工具类
  *
  * @author zhangsl
  * @date 2019/12/13 11:50 
  * @version 1.0
  */
object DateUtils {
  /**
    * 获取前一天日期
    * @param day
    * @return
    */
  def getTimeAgo(day:String):String = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date: Date = format.parse(day)
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE,-1)
    val agoDay: String = format.format(calendar.getTime)
    agoDay
  }

  def main(args: Array[String]): Unit = {
    val str: String = getTimeAgo("20191212")
    println(str)
  }
}
