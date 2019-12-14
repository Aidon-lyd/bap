package dmp.util

import com.typesafe.config.{Config, ConfigFactory}

object ConfigHelper {
  //加载数据文件
  private val config: Config = ConfigFactory.load()
  //App Info
  lazy val sparkAppName: String = config.getString("spark.appname")

  //spark parameters
  lazy val sparkMaster: String = config.getString("spark.master")

  lazy val sparkParameters: List[(String, String)] = List(
    ("spark.worker.timeout", config.getString("spark.worker.timeout")),
    ("spark.cores.max", config.getString("spark.cores.max")),
    ("spark.rpc.askTimeout", config.getString("spark.rpc.askTimeout")),
    ("spark.network.timeout", config.getString("spark.network.timeout")),
    ("spark.task.maxFailures", config.getString("spark.task.maxFailures")),
    ("spark.speculation", config.getString("spark.speculation")),
    ("spark.driver.allowMultipleContexts", config.getString("spark.driver.allowMultipleContexts")),
    ("spark.serializer", config.getString("spark.serializer")),
    ("spark.buffer.pageSize", config.getString("spark.buffer.pageSize")),
    ("spark.debug.maxToStringFields", config.getString("spark.debug.maxToStringFields")),
    ("spark.sql.shuffle.partitions", config.getString("spark.sql.shuffle.partitions")),
    ("spark.sql.autoBroadcastJoinThreshold", config.getString("spark.sql.autoBroadcastJoinThreshold")),
    ("es.nodes","node245"),
    ("es.port","9200"),
    ("es.index.auto.create", "true")
  )

  //input data
  lazy val adDataPath: String = config.getString("addata.path")

  //input data parquet
  lazy val adFormatData: String = config.getString("formatdata.path")

  //input appdic
  lazy val appNameDic: String = config.getString("appDic.path")

  //input deviceDic
  lazy val deviceDic: String = config.getString("device.path")

  //input stopkeyword
  lazy val stopwordDic: String = config.getString("stopwords.path")

  //识别字段
  lazy val idFields: String = config.getString("non.empty.id")
  lazy val filterFields: String = idFields.split(",")
    .map(field =>
      s"""
         |$field != '' ""
       """.stripMargin)
    .mkString("or")

  // $field ne '' 可替换为 trim($field) <>




  //output json
  lazy val outputJson: String = config.getString("json.path")

  //高德Api
  private lazy val gaoDeKey: String = config.getString("gaoDe.app.key")
  private lazy val gaoDeTempUrl: String = config.getString("gaoDe.url")
  lazy val gaoDeUrl: String = s"$gaoDeTempUrl&key=$gaoDeKey"

  // GeoHash
  lazy val keyLength: Int = config.getInt("geohash.key.length")
}
