package com.jiangxi.tags

import com.jiangxi.`trait`.Tags
import com.jiangxi.entry.logBean
/**
  * 渠道标签
  *（标签格式：CNxxxx->1）xxxx为渠道ID
  * @param args
  * @return
  */
object channelTag extends  Tags{
  override def makeTag(args: Any*): List[(String, Double)] = {
    var  list = List[(String,Double)]()
    if(args.length>0){
      var log = args(0).asInstanceOf[logBean]
      if(log.channelid !=null){
        list :+=("CN".concat(log.channelid)->1.0)
      }
    }
    list
  }
}
