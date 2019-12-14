package dmp.tags

import dmp.traits.TagMaker
import org.apache.spark.sql.Row

//关键字标签
object KeywordsTag extends TagMaker{
  override def make(row: Row, dic: collection.Map[String, String]): Map[String, Double] = {
    val keywords: String = row.getAs[String]("keywords")
    if(keywords == null || keywords.length == 0){
      Map[String, Double]("无"->1.0)
    }else{
      val arr: Array[String] = keywords
        .split("\\|")
        .filter(words => words.length > 0)

      var map: Map[String, Double] = Map[String, Double]()
      for( i<-  0 to arr.length-1){
        if(arr(i).length>=3 && arr(i).length<=8 && dic.getOrElse(arr(i), "1") == "1")
          map.+=("K"+arr(i) -> 1.0)
      }
      map
    }

//      .filter(words => dic.getOrElse(words, "1") == "1")
//      .map(word => "K"+word -> 1.0)
//      .toMap
  }
}
