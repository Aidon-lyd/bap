package com.jiangxi.tags

import com.jiangxi.`trait`.Tags
import com.jiangxi.entry.logBean
/**
  * 5）关键词（标签格式：Kxxx->1）xxx为关键字。关键词个数不能少于3个字符，且不能超过8个字符；关键字中如包含”|”,则分割成数组，转化成多个关键字标签
  */
object keyWordsTag extends Tags{
  override def makeTag(args: Any*): List[(String, Double)] = {
    var  list = List[(String,Double)]()
    if(args.length>0){
      //获取数据
      val log: logBean = args(0).asInstanceOf[logBean]
      if(log.keywords!=null){
        val fields: Array[String] = log.keywords.split("\\|")
        //        for(word <- fields){
        //          if(word.length >= 3 && word.length <= 8){
        //            list +=("K".concat(word) -> 1)
        //          }
        //        }
        fields.filter(word=>{
          word.length>=3 && word.length <9
        }).foreach(str=>{
          list :+= ("K".concat(str.replace(":","")) -> 1.0)
        })
      }
    }
 list
  }
}
