package ods_release

import scala.collection.mutable.ArrayBuffer

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/6
  * Time: 16:06
  *
  * @author zbs
  * @version 1.0
  */
object ODS_Fields {
  def InitArr_Fields = {
    var columns = new ArrayBuffer[String]()
    columns += ("cast (value[0 ] as STRING ) as sessionid")
    columns += ("cast (value[1 ] as INT ) as advertisersid ")
    columns += ("cast (value[2 ] as INT ) as adorderid ")
    columns += ("cast (value[3 ] as INT ) as adcreativeid")
    columns += ("cast (value[4 ] as INT ) as adplatformproviderid")
    columns += ("cast (value[5 ] as STRING ) as sdkversion ")
    columns += ("cast (value[6 ] as STRING ) as adplatformkey")
    columns += ("cast (value[7 ] as INT ) as putinmodeltype")
    columns += ("cast (value[8 ] as INT ) as requestmode ")
    columns += ("cast (value[9 ] as DOUBLE ) as adprice")
    columns += ("cast (value[10 ] as DOUBLE ) as adppprice ")
    columns += ("cast (value[11 ] as STRING ) as requestdate ")
    columns += ("cast (value[12 ] as STRING ) as ip")
    columns += ("cast (value[13 ] as STRING ) as appid ")
    columns += ("cast (value[14 ] as STRING ) as appname ")
    columns += ("cast (value[15 ] as STRING ) as uuid")
    columns += ("cast (value[16 ] as STRING ) as device")
    columns += ("cast (value[17 ] as INT ) as client ")
    columns += ("cast (value[18 ] as STRING ) as osversion ")
    columns += ("cast (value[19 ] as STRING ) as density ")
    columns += ("cast (value[20 ] as INT ) as pw ")
    columns += ("cast (value[21 ] as INT ) as ph ")
    columns += ("cast (value[22 ] as STRING ) as long")
    columns += ("cast (value[23 ] as STRING ) as lat ")
    columns += ("cast (value[24 ] as STRING ) as provincename")
    columns += ("cast (value[25 ] as STRING ) as cityname")
    columns += ("cast (value[26 ] as INT ) as ispid")
    columns += ("cast (value[27 ] as STRING ) as ispname ")
    columns += ("cast (value[28 ] as INT ) as networkmannerid")
    columns += ("cast (value[29 ] as STRING ) as networkmannername ")
    columns += ("cast (value[30 ] as INT ) as iseffective")
    columns += ("cast (value[31 ] as INT ) as isbilling")
    columns += ("cast (value[32 ] as INT ) as adspacetype")
    columns += ("cast (value[33 ] as STRING ) as adspacetypename ")
    columns += ("cast (value[34 ] as INT ) as devicetype ")
    columns += ("cast (value[35 ] as INT ) as processnode")
    columns += ("cast (value[36 ] as INT ) as apptype")
    columns += ("cast (value[37 ] as STRING ) as district")
    columns += ("cast (value[38 ] as INT ) as paymode")
    columns += ("cast (value[39 ] as INT ) as isbid")
    columns += ("cast (value[40 ] as DOUBLE ) as bidprice ")
    columns += ("cast (value[41 ] as DOUBLE ) as winprice ")
    columns += ("cast (value[42 ] as INT ) as iswin")
    columns += ("cast (value[43 ] as STRING ) as cur ")
    columns += ("cast (value[44 ] as DOUBLE ) as rate ")
    columns += ("cast (value[45 ] as DOUBLE ) as cnywinprice")
    columns += ("cast (value[46 ] as STRING ) as imei")
    columns += ("cast (value[47 ] as STRING ) as mac ")
    columns += ("cast (value[48 ] as STRING ) as idfa")
    columns += ("cast (value[49 ] as STRING ) as openudid")
    columns += ("cast (value[50 ] as STRING ) as androidid ")
    columns += ("cast (value[51 ] as STRING )  as rtbprovince ")
    columns += ("cast (value[52 ] as STRING ) as rtbcity ")
    columns += ("cast (value[53 ] as STRING ) as rtbdistrict ")
    columns += ("cast (value[54 ] as STRING ) as rtbstreet ")
    columns += ("cast (value[55 ] as STRING ) as storeurl")
    columns += ("cast (value[56 ] as STRING ) as realip")
    columns += ("cast (value[57 ] as INT ) as isqualityapp ")
    columns += ("cast (value[58 ] as DOUBLE ) as bidfloor ")
    columns += ("cast (value[59 ] as INT ) as aw ")
    columns += ("cast (value[60 ] as INT ) as ah ")
    columns += ("cast (value[61 ] as STRING ) as imeimd5 ")
    columns += ("cast (value[62 ] as STRING ) as macmd5")
    columns += ("cast (value[63 ] as STRING ) as idfamd5 ")
    columns += ("cast (value[64 ] as STRING ) as openudidmd5 ")
    columns += ("cast (value[65 ] as STRING ) as androididmd5")
    columns += ("cast (value[66 ] as STRING ) as imeisha1")
    columns += ("cast (value[67 ] as STRING ) as macsha1 ")
    columns += ("cast (value[68 ] as STRING ) as idfasha1")
    columns += ("cast (value[69 ] as STRING ) as openudidsha1")
    columns += ("cast (value[70 ] as STRING ) as androididsha1 ")
    columns += ("cast (value[71 ] as STRING ) as uuidunknow")
    columns += ("cast (value[72 ] as STRING ) as userid")
    columns += ("cast (value[73 ] as INT ) as iptype ")
    columns += ("cast (value[74 ] as DOUBLE ) as initbidprice ")
    columns += ("cast (value[75 ] as DOUBLE ) as adpayment")
    columns += ("cast (value[76 ] as DOUBLE ) as agentrate")
    columns += ("cast (value[77 ] as DOUBLE ) as lomarkrate ")
    columns += ("cast (value[78 ] as DOUBLE ) as adxrate")
    columns += ("cast (value[79 ] as STRING ) as title ")
    columns += ("cast (value[80 ] as STRING ) as keywords")
    columns += ("cast (value[81 ] as STRING ) as tagid ")
    columns += ("cast (value[82 ] as STRING ) as callbackdate")
    columns += ("cast (value[83 ] as STRING ) as channelid ")
    columns += ("cast (value[84 ] as INT ) as mediatype")
    columns
  }
}
