package bean

object CaseClass {

  /**
    * The Format for Data Of Logs
    *
    * @param sessionid
    * @param advertisersid
    * @param adorderid
    * @param adcreativeid
    * @param adplatformproviderid
    * @param sdkversion
    * @param adplatformkey
    * @param putinmodeltype
    * @param requestmode
    * @param adprice
    * @param adppprice
    * @param requestdate
    * @param ip
    * @param appid
    * @param appname
    * @param uuid
    * @param device
    * @param client
    * @param osversion
    * @param density
    * @param pw
    * @param ph
    * @param longitude
    * @param lat
    * @param provincename
    * @param cityname
    * @param ispid
    * @param ispname
    * @param networkmannerid
    * @param networkmannername
    * @param iseffective
    * @param isbilling
    * @param adspacetype
    * @param adspacetypename
    * @param devicetype
    * @param processnode
    * @param apptype
    * @param district
    * @param paymode
    * @param isbid
    * @param bidprice
    * @param winprice
    * @param iswin
    * @param cur
    * @param rate
    * @param cnywinprice
    * @param imei
    * @param mac
    * @param idfa
    * @param openudid
    * @param androidid
    * @param rtbprovince
    * @param rtbcity
    * @param rtbdistrict
    * @param rtbstreet
    * @param storeurl
    * @param realip
    * @param isqualityapp
    * @param bidfloor
    * @param aw
    * @param ah
    * @param imeimd5
    * @param macmd5
    * @param idfamd5
    * @param openudidmd5
    * @param androididmd5
    * @param imeisha1
    * @param macsha1
    * @param idfasha1
    * @param openudidsha1
    * @param androididsha1
    * @param uuidunknow
    * @param userid
    * @param iptype
    * @param initbidprice
    * @param adpayment
    * @param agentrate
    * @param lomarkrate
    * @param adxrate
    * @param title
    * @param keywords
    * @param tagid
    * @param callbackdate
    * @param channelid
    * @param mediatype
    */
  case class LogData
  (
    sessionid: String,
    advertisersid: Int,
    adorderid: Int,
    adcreativeid: Int,
    adplatformproviderid: Int,
    sdkversion: String,
    adplatformkey: String,
    putinmodeltype: Int,
    requestmode: Int,
    adprice: Double,
    adppprice: Double,
    requestdate: String,
    ip: String,
    appid: String,
    appname: String,
    uuid: String,
    device: String,
    client: Int,
    osversion: String,
    density: String,
    pw: Int,
    ph: Int,
    longitude: String,
    lat: String,
    provincename: String,
    cityname: String,
    ispid: Int,
    ispname: String,
    networkmannerid: Int,
    networkmannername: String,
    iseffective: Int,
    isbilling: Int,
    adspacetype: Int,
    adspacetypename: String,
    devicetype: Int,
    processnode: Int,
    apptype: Int,
    district: String,
    paymode: Int,
    isbid: Int,
    bidprice: Double,
    winprice: Double,
    iswin: Int,
    cur: String,
    rate: Double,
    cnywinprice: Double,
    imei: String,
    mac: String,
    idfa: String,
    openudid: String,
    androidid: String,
    rtbprovince: String,
    rtbcity: String,
    rtbdistrict: String,
    rtbstreet: String,
    storeurl: String,
    realip: String,
    isqualityapp: Int,
    bidfloor: Double,
    aw: Int,
    ah: Int,
    imeimd5: String,
    macmd5: String,
    idfamd5: String,
    openudidmd5: String,
    androididmd5: String,
    imeisha1: String,
    macsha1: String,
    idfasha1: String,
    openudidsha1: String,
    androididsha1: String,
    uuidunknow: String,
    userid: String,
    iptype: Int,
    initbidprice: Double,
    adpayment: Double,
    agentrate: Double,
    lomarkrate: Double,
    adxrate: Double,
    title: String,
    keywords: String,
    tagid: String,
    callbackdate: String,
    channelid: String,
    mediatype: Int
  )

}
