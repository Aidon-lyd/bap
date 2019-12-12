package com.cmcc.dmp.filehand

import com.cmcc.dmp.Utils.{DmpLOGObjrct, NumberUtils, SparkHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/6 14:07 
  * @version 1.0
  */
case class People(name:String,miaoshu:String,ww:String,qq:Int)
case class DmpLOG(
                   sessionid: String,  //会话标识
                   advertisersid: Int,  //广告主 id
                   adorderid: Int,  //广告 id
                   adcreativeid: Int,  //广告创意 id  //( >= 200000 : dsp)
                   adplatformproviderid: Int,  //广告平台商 id  //(>= 100000: rtb)
                   sdkversion: String,  //sdk 版本号
                   adplatformkey: String,  //平台商 key
                   putinmodeltype: Int,  //针对广告主的投放模式,1：展示量投放 2：点击量投放
                   requestmode: Int,  //数据请求方式（1:请求、2:展示、3:点击）
                   adprice: Double,  //广告价格
                   adppprice: Double,  //平台商价格
                   requestdate: String,  //请求时间,格式为：yyyy-m-dd hh:mm:ss
                   ip: String,  //设备用户的真实 ip 地址
                   appid: String,  //应用 id
                   appname: String,  //应用名称
                   uuid: String,  //设备唯一标识
                   device: String,  //设备型号，如 htc、iphone
                   client: Int,  //设备类型 （1：android 2：ios 3：wp）
                   osversion: String,  //设备操作系统版本
                   density: String,  //设备屏幕的密度
                   pw: Int,  //设备屏幕宽度
                   ph: Int,  //设备屏幕高度
                   longitude: String,  //设备所在经度
                   lat: String,  //设备所在纬度
                   provincename: String,  //设备所在省份名称
                   cityname: String,  //设备所在城市名称
                   ispid: Int,  //运营商 id
                   ispname: String,  //运营商名称
                   networkmannerid: Int,  //联网方式 id
                   networkmannername: String,  //联网方式名称
                   iseffective: Int,  //有效标识（有效指可以正常计费的）(0：无效 1：有效
                   isbilling: Int,  //是否收费（0：未收费 1：已收费）
                   adspacetype: Int,  //广告位类型（1：banner 2：插屏 3：全屏）
                   adspacetypename: String,  //广告位类型名称（banner、插屏、全屏）
                   devicetype: Int,  //设备类型（1：手机 2：平板）
                   processnode: Int,  //流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
                   apptype: Int,  //应用类型 id
                   district: String,  //设备所在县名称
                   paymode: Int,  //针对平台商的支付模式，1：展示量投放(CPM) 2：点击
                   isbid: Int,  //是否 rtb
                   bidprice: Double,  //rtb 竞价价格
                   winprice: Double,  //rtb 竞价成功价格
                   iswin: Int,  //是否竞价成功
                   cur: String,  //values:usd|rmb 等
                   rate: Double,  //汇率
                   cnywinprice: Double,  //rtb 竞价成功转换成人民币的价格
                   imei: String,  //Imei（移动设备识别码）
                   mac: String,  //Mac（苹果设备）
                   idfa: String,  //Idfa（广告标识符）
                   openudid: String,  //Openudid（UDID的第三方解决方案）
                   androidid: String,  //Androidid（安卓设备的唯一id）
                   rtbprovince: String,  //rtb 省
                   rtbcity: String,  //rtb 市
                   rtbdistrict: String,  //rtb 区
                   rtbstreet: String,  //rtb 街 道
                   storeurl: String,  //app 的市场下载地址
                   realip: String,  //真实 ip
                   isqualityapp: Int,  //优选标识
                   bidfloor: Double,  //底价
                   aw: Int,  //广告位的宽
                   ah: Int,  //广告位的高
                   imeimd5: String,  //imei_md5
                   macmd5: String,  //mac_md5
                   idfamd5: String,  //idfa_md5
                   openudidmd5: String,  //openudid_md5
                   androididmd5: String,  //androidid_md5
                   imeisha1: String,  //imei_sha1
                   macsha1: String,  //mac_sha1
                   idfasha1: String,  //idfa_sha1
                   openudidsha1: String,  //openudid_sha1
                   androididsha1: String,  //androidid_sha1
                   uuidunknow: String,  //uuid_unknow tanx 密文
                   userid: String,  //平台用户 id
                   iptype: Int,  //表示 ip 类型
                   initbidprice: Double,  //初始出价
                   adpayment: Double,  //转换后的广告消费
                   agentrate: Double,  //代理商利润率
                   lomarkrate: Double,  //代理利润率
                   adxrate: Double,  //媒介利润率
                   title: String,  //标题
                   keywords: String,  //关键字
                   tagid: String,  //广告位标识(当视频流量时值为视频 ID 号)
                   callbackdate: String,  //回调时间 格式为:YYYY/mm/dd hh:mm:ss
                   channelid: String,  //频道 ID
                   mediatype: Int  //媒体类型：1 长尾媒体 2 视频媒体 3 独立媒体  //默认:1
                 )
object TextToParquet {
  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val master = "local[*]"
    //val savePath = "out"
    val savePath = "outAnli"
    val conf: SparkConf = SparkHelper.getSparkConf(appName,master)
    //设置 kryo 序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[DmpLOG],classOf[People]))
    // 创建 SparkContext 上下文对象
    val context: SparkContext = new SparkContext(conf)
    // 读取text文件
    //val logDataRDD: RDD[String] = context.textFile("log/2016*")
    val logDataRDD: RDD[String] = context.textFile("log/anli/2016-10-01_06_p1_invalid.1475274123982.log")
    val DmpLogRDD: RDD[DmpLOG] = logDataRDD
      .mapPartitions(_.map(line => {
        val arr = line.split(",",-1)
       DmpLOGObjrct.getDmpLOG(arr)
      })
      )
    // 获取 SparkSession 对象
    val spark: SparkSession = SparkHelper.getSpark(conf)
    import spark.implicits._
    val dataFrame: DataFrame = DmpLogRDD.toDF
    dataFrame.write.save(savePath)

    spark.stop()
    context.stop()

  }
}
