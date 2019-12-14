package com.jiangxi.tags
import org.apache.spark.broadcast.Broadcast
import com.jiangxi.`trait`.Tags
import com.jiangxi.entry.logBean

/**
  * 4）设备：操作系统|联网方式|运营商
  */
object deviceTag extends Tags{
  /**
    * 打标签的方法
    * 设备标签：
    * 1）设备操作系统
    * 2）设备联网方式标签
    * 3）设备运营商方案标签
    */
       /*映射文件格式
     1	D00010001	Android
     2	D00010002	IOS
     3	D00010003	WIN
     4	D00010004	其他
     WIFI	D00020001	WIFI
     */
  override def makeTag(args: Any*): List[(String, Double)] = {
    var list = List[(String,Double)]()
         if(args.length>0){
           //获取数据
           var  log :logBean= args(0).asInstanceOf[logBean]
           //val deviceDict: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String,String]]]
           //获取设备操作系统
           log.client match{
             case 1 => list :+= ("D00010001",1.0)
             case 2 => list :+= ("D00010002",1.0)
             case 3 => list :+= ("D00010003",1.0)
             case _ => list :+= ("D00010004",1.0)
           }
           //获取设备联网方式
           log.ispname match {
             case "WIFI" => list :+=("D00020001",1.0)
             case "4G" => list :+= ("D00020002",1.0)
             case "3G" => list :+= ("D00020003",1.0)
             case "2G" => list :+= ("D00020004",1.0)
             case  _   =>  list :+= ("D00020005",1.0)
           }
           //获取设备运营商
           log.networkmannername match {
             case "移动" => list :+= ("D00030001",1.0)
             case "联通" => list :+= ("D00030002",1.0)
             case "电信" => list :+= ("D00030003",1.0)
             case   _   =>  list :+= ("D00030004",1.0)
           }
         }
   list
  }
}
/* @param args
*          args0:Logs
*          args1:Map[String,String]
*          key:WIFI
*          value: D00020001
* @return
*
* //注意在Map中.get("4")获取到的值是Option类型，需要再次.get()拿到里面的值*/