package com.jiangxi.tags
import com.jiangxi.`trait`.Tags
import com.jiangxi.entry.logBean
import org.apache.spark.broadcast.Broadcast
/**
  * 2）APP名称（标签格式：APPxxxx->1）xxxx为APP的名称，使用缓存文件appname_dict进行名称转换；
  */
object appTag  extends Tags{
  /**
    * 打标签的方法
    * 给APP打标签
    * @param args
    *   args0:Logs
    *   args1:Map[String,String]:
    *           key:appID
    *           value:appName
    * @return
    */
  override def makeTag(args: Any*): List[(String,Double)] = {
    var list = List[(String, Double)]()

    val log = args(0).asInstanceOf[logBean]
    val appDict = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    val appname: String = log.appname
    val appid: String = log.appid
    //val appDict: List[String, String] = args(1).asInstanceOf[List[String,String]]
    //根据标签格式形成标签
    if (appname != null && !appname.isEmpty) {
      list :+= ("APP" + appname -> 1.0)
    } else if (appid != null && !appid.isEmpty) {
      list :+= ("APP" + appDict.value.getOrElse(appid, "未知") -> 1.0)
    }
    list
  }
}
/*
* val appDict: Map[String, String] = args(1).asInstanceOf[Map[String,String]]
      val appName = appDict.getOrElse(log.appid,log.appname)
      if(StringUtils.isNotEmpty(appName) && !"".equals(appName)){
         map += ("APP"+appName -> 1)
      }
*
*
*/