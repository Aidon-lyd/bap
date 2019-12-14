package com.jiangxi.tags

import com.jiangxi.`trait`.Tags
import com.jiangxi.entry.logBean
/**
  * 6)地域标签（省标签格式：ZPxxx->1，地市标签格式：ZCxxx->1）xxx为省或市名称
  */
object areaTag extends Tags{
  override def makeTag(args: Any*): List[(String, Double)] = {
    var list = List[(String,Double)]()
    if(args.length>0){
    //获取数据
    var log = args(0).asInstanceOf[logBean]
    //provincename 设备所在省份名称
    if(log.provincename != null){
      list :+=("ZP"+log.provincename -> 1.0)
    }
    //设备所在的城市
    if(log.cityname != null){
      list :+=("ZC"+log.cityname -> 1.0)
    }
    }
    list
  }
}
