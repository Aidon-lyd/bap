package dmp.tags


import dmp.traits.Processor
import dmp.util.{ConfigHelper, HbaseUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


//标签汇总
object TagContext extends Processor{
  override def processor(spark: SparkSession): Unit = {

    //定义参数
    val sourcePath: String = ConfigHelper.adFormatData
    val appDicFilePath: String = ConfigHelper.appNameDic
    val devicePath: String = ConfigHelper.deviceDic
    val stopWordPath: String = ConfigHelper.stopwordDic



    //读数据
    val data: DataFrame = spark.read.parquet(sourcePath)


    import spark.implicits._

    //读app信息(文件)，转换为广播变量(优化)
    val appDicMap: collection.Map[String, String] = spark.sparkContext.textFile(appDicFilePath)
      .filter(line => line.split("\t").length >= 5)
      .map(line => {
        val arr: Array[String] = line.split("\t")
        var appid = ""
        if(arr(3).startsWith("A"))
          appid = arr(4)
        else
          appid = arr(3)
        val appName = arr(1)
        (appid, appName)
      }).collectAsMap()
    val appdicBC: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(appDicMap)

    //读设备字典信息(文件)，转换为广播变量(优化)
    val deviceMap: collection.Map[String, String] = spark.sparkContext.textFile(devicePath)
      .map(line => {
        val arr: Array[String] = line.split("##")
        (arr(0), arr(1))
      }).collectAsMap()
    val deviceBC: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(deviceMap)

    //关键词信息(文件)，转换为广播变量(优化)
    val keywordMap: collection.Map[String, String] = spark.sparkContext.textFile(stopWordPath)
      .map(line => {
        (line, "0")
      }).collectAsMap()
    val stopWordBC: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(keywordMap)

    //处理数据，生成标签
    val logTags: RDD[(String, String)] = data.filter(ConfigHelper.filterFields)
      .rdd
      .map(row => {
        //ad channel
        val adTag: Map[String, Double] = AdTypeTag.make(row)
        val channelTag: Map[String, Double] = ChannelTag.make(row)

        //App Device
        val appNameTag: Map[String, Double] = AppNameTag.make(row, appdicBC.value)
        val deviceTag: Map[String, Double] = DeviceTag.make(row, deviceBC.value)

        //key loc
        val keyWordsTags: Map[String, Double] = KeywordsTag.make(row, stopWordBC.value)
        val locTag: Map[String, Double] = LocTag.make(row)

        //商圈标签
        val tradeAreaTagsTmp: Map[String, Double] = TradingAreaTag.make(row)

        //tradeAreaTagsTmp.foreach(println)
        val tradeAreaTags: Map[String, Double] = Map[String, Double]()
        for((key , value) <- tradeAreaTagsTmp){
          if(!"SQ无".equals(key)){
           // println((key -> value).toString())
            tradeAreaTags .+ (key -> value)
          }
        }

        //结合所有标签
        val tags: Map[String, Double] = adTag ++ channelTag ++ appNameTag ++ deviceTag ++ keyWordsTags ++ locTag ++ tradeAreaTags

        //获取用户的key
        val allIds: List[String] = getUserIDs(row)

        (allIds.toString() , tags.toList.toString())
      })
    
    logTags.collect().foreach(println)

    //存储到Hbase，采用批处理的方式
    //create 'Tags'
    //alter 'Tags',{NAME => 'Day01'}
//    val mapTags: collection.Map[String, String] = logTags.collectAsMap()
//    val hashMap = new java.util.HashMap[String,String]()
//    for((key,value)<-mapTags){
//      hashMap.put(key,value)
//    }

//    HbaseUtils.put("Tags","Day01","PersonTags",hashMap)




    //保存数据到hbase
    //这个方法没写对
//    Save2Hbase.saveToHbase(logTags)

    //Yuniko的方法
    /**val conf = new Configuration
    conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZK)
    val connection = ConnectionFactory.createConnection(conf)
    val hBaseAdmin  = connection.getAdmin.asInstanceOf[HBaseAdmin]

    val table:HTable = connection.getTable(TableName.valueOf(Constant.HBASE_TAG_TABLE)).asInstanceOf[HTable]
    table.setAutoFlush(false)
    val putList = new java.util.LinkedList[Put]()
    val family = Bytes.toBytes("cf1")
    val column =Bytes.toBytes("TagValue")
    list.foreach(value => {
      val put = new Put(Bytes.toBytes(value.getString(0)))
      put.addColumn(family, column, Bytes.toBytes(value.getString(1)))
      putList.add(put)
    })
    table.put(putList)

    table.flushCommits()
    table.close()*/


    spark.stop()

  }

  def getUserIDs(row: Row): List[String] = {
    val fields: String = ConfigHelper.idFields
    fields.split(",")
      .map(field => (field, row.getAs[String](field)))
      .filter{case (key, value) => StringUtils.isNotBlank(value)}
      .map{case (key, value) => s"$key::$value"}
      .toList
  }
}
