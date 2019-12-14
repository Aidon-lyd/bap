package com.jiangxi.entry
import com.jiangxi.util.StringUtil
import org.apache.commons.lang.StringUtils
case class logBean(
                    val sessionid: String, //会话标识
                    val advertisersid: Int, //广告主id
                    val adorderid: Int, //广告id
                    val adcreativeid: Int, //广告创意id   ( >= 200000 : dsp ,  < 200000 oss)
                    val adplatformproviderid: Int, //广告平台商id  (>= 100000: rtb  , < 100000 : api )
                    val sdkversionnumber: String, //sdk版本号
                    val adplatformkey: String, //平台商key
                    val putinmodeltype: Int, //针对广告主的投放模式,1：展示量投放 2：点击量投放
                    val requestmode: Int, //数据请求方式（1:请求、2:展示、3:点击）
                    val adprice: Double, //广告价格
                    val adppprice: Double, //平台商价格
                    val requestdate: String, //请求时间,格式为：yyyy-m-dd hh:mm:ss
                    val ip: String, //设备用户的真实ip地址
                    val appid: String, //应用id
                    val appname: String, //应用名称
                    val uuid: String, //设备唯一标识，比如imei或者androidid等
                    val device: String, //设备型号，如htc、iphone
                    val client: Int, //设备类型 （1：android 2：ios 3：wp）
                    val osversion: String, //设备操作系统版本，如4.0
                    val density: String, //备屏幕的密度 android的取值为0.75、1、1.5,ios的取值为：1、2
                    val pw: Int, //设备屏幕宽度
                    val ph: Int, //设备屏幕高度
                    val longitude: String, //设备所在经度
                    val lat: String, //设备所在纬度
                    val provincename: String, //设备所在省份名称
                    val cityname: String, //设备所在城市名称
                    val ispid: Int, //运营商id
                    val ispname: String, //运营商名称
                    val networkmannerid: Int, //联网方式id
                    val networkmannername: String, //联网方式名称
                    val iseffective: Int, //有效标识（有效指可以正常计费的）(0：无效 1：有效)
                    val isbilling: Int, //是否收费（0：未收费 1：已收费）
                    val adspacetype: Int, //广告位类型（1：banner 2：插屏 3：全屏）
                    val adspacetypename: String, //广告位类型名称（banner、插屏、全屏）
                    val devicetype: Int, //设备类型（1：手机 2：平板）
                    val processnode: Int, //流程节点（1：请求量kpi 2：有效请求 3：广告请求）
                    val apptype: Int, //应用类型id
                    val district: String, //设备所在县名称
                    val paymode: Int, //针对平台商的支付模式，1：展示量投放(CPM) 2：点击量投放(CPC)
                    val isbid: Int, //是否rtb
                    val bidprice: Double, //rtb竞价价格
                    val winprice: Double, //rtb竞价成功价格
                    val iswin: Int, //是否竞价成功
                    val cur: String, //values:usd|rmb等
                    val rate: Double, //汇率
                    val cnywinprice: Double, //rtb竞价成功转换成人民币的价格
                    val imei: String, //imei
                    val mac: String, //mac
                    val idfa: String, //idfa
                    val openudid: String, //openudid
                    val androidid: String, //androidid
                    val rtbprovince: String, //rtb 省
                    val rtbcity: String, //rtb 市
                    val rtbdistrict: String, //rtb 区
                    val rtbstreet: String, //rtb 街道
                    val storeurl: String, //app的市场下载地址
                    val realip: String, //真实ip
                    val isqualityapp: Int, //优选标识
                    val bidfloor: Double, //底价
                    val aw: Int, //广告位的宽
                    val ah: Int, //广告位的高
                    val imeimd5: String, //imei_md5
                    val macmd5: String, //mac_md5
                    val idfamd5: String, //idfa_md5
                    val openudidmd5: String, //openudid_md5
                    val androididmd5: String, //androidid_md5
                    val imeisha1: String, //imei_sha1
                    val macsha1: String, //mac_sha1
                    val idfasha1: String, //idfa_sha1
                    val openudidsha1: String, //openudid_sha1
                    val androididsha1: String, //androidid_sha1
                    val uuidunknow: String, //uuid_unknow tanx密文
                    val userid: String, //平台用户id
                    val iptype: Int, //表示ip库类型，1为点媒ip库，2为广告协会的ip地理信息标准库，默认为1
                    val initbidprice: Double, //初始出价
                    val adpayment: Double, //转换后的广告消费（保留小数点后6位）
                    val agentrate: Double, //代理商利润率
                    val lomarkrate: Double, //代理利润率
                    val adxrate: Double, //媒介利润率
                    val title: String, //标题
                    val keywords: String, //关键字
                    val tagid: String, //广告位标识(当视频流量时值为视频ID号)
                    val callbackdate: String, //回调时间 格式为:YYYY/mm/dd hh:mm:ss
                    val channelid: String, //频道ID
                    val mediatype: Int
                   )extends  Serializable {}
/**
  * 只要给我们传过来一条数据，我们就可以通过line2Log转换成一个日志对象
  */
object logBean {

  //创建空对象
  def makeLogs(): logBean = {
    new logBean("", 0, 0, 0, 0, ""
      , "", 0, 0, 0.0, 0.0, "", "",
      "", "", "", "", 0, "", "", 0, 0, "",
      "", "", "", 0, "", 0, "", 0,
      0, 0, "", 0, 0, 0, "",
      0, 0, 0.0, 0.0, 0, "", 0.0, 0.0, "",
      "", "", "", "", "", "", "", "", "",
      "", 0, 0.0, 0, 0, "", "", "", "", "", "",
      "", "", "", "", "", "", 0, 0.0, 0.0,
      0.0, 0.0, 0.0, "", "", "", "", "", 0)
  }

  def line2log(lines: String): logBean = {
    if (StringUtils.isNotEmpty(lines)) {
      val arr = lines.split(",", -1)
      new logBean(
        arr(0),
        StringUtil.StringtoInt(arr(1)),
        StringUtil.StringtoInt(arr(2)),
        StringUtil.StringtoInt(arr(3)),
        StringUtil.StringtoInt(arr(4)),
        arr(5),
        arr(6),
        StringUtil.StringtoInt(arr(7)),
        StringUtil.StringtoInt(arr(8)),
        StringUtil.StringtoDouble(arr(9)),
        StringUtil.StringtoDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StringUtil.StringtoInt(arr(17)),
        arr(18),
        arr(19),
        StringUtil.StringtoInt(arr(20)),
        StringUtil.StringtoInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StringUtil.StringtoInt(arr(26)),
        arr(27),
        StringUtil.StringtoInt(arr(28)),
        arr(29),
        StringUtil.StringtoInt(arr(30)),
        StringUtil.StringtoInt(arr(31)),
        StringUtil.StringtoInt(arr(32)),
        arr(33),
        StringUtil.StringtoInt(arr(34)),
        StringUtil.StringtoInt(arr(35)),
        StringUtil.StringtoInt(arr(36)),
        arr(37),
        StringUtil.StringtoInt(arr(38)),
        StringUtil.StringtoInt(arr(39)),
        StringUtil.StringtoDouble(arr(40)),
        StringUtil.StringtoDouble(arr(41)),
        StringUtil.StringtoInt(arr(42)),
        arr(43),
        StringUtil.StringtoDouble(arr(44)),
        StringUtil.StringtoDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StringUtil.StringtoInt(arr(57)),
        StringUtil.StringtoDouble(arr(58)),
        StringUtil.StringtoInt(arr(59)),
        StringUtil.StringtoInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StringUtil.StringtoInt(arr(73)),
        StringUtil.StringtoDouble(arr(74)),
        StringUtil.StringtoDouble(arr(75)),
        StringUtil.StringtoDouble(arr(76)),
        StringUtil.StringtoDouble(arr(77)),
        StringUtil.StringtoDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StringUtil.StringtoInt(arr(84))
      )} else {
      //万一没满足条件，我们后面的代码就无法运行了。所以要创建空对象
      makeLogs()
    }
  }
}