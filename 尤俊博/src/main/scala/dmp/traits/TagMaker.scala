package dmp.traits

import org.apache.spark.sql.Row

trait TagMaker {
  //出入一行数据：输出位标签，格式为：标签 -> 权重
  def make(row: Row, dic: collection.Map[String,String]=null): Map[String, Double]
}
