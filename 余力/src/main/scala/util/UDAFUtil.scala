package util

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.json4s.NoTypeHints
import org.json4s.native.Serialization._
import org.json4s.native.Serialization



object UDAFUtil {

  //The Following Two UDAF Can be Optimized By using Variable Parameters
  /**
    * UDAF For Merging Common Tag
    */
  object mergeCommonTag extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      StructType(
        StructField("commonTag", StringType) ::
          Nil
      )
    }

    override def bufferSchema: StructType = {
      StructType(
        StructField("mergedCommonTag", MapType(StringType, IntegerType)) ::
          Nil
      )
    }

    override def dataType: DataType = {
      StringType
    }

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, Map[String, Int]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val keywords: String = input.getString(0)
      var mergedCommonTagMap: collection.Map[String, Int] = buffer.getMap[String, Int](0)

      if (mergedCommonTagMap.keySet.contains(keywords))
        mergedCommonTagMap += ((keywords, mergedCommonTagMap.get(keywords).get + 1))
      else
        mergedCommonTagMap += ((keywords, 1))

      buffer.update(0, mergedCommonTagMap)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      var buf1MergedCommonTagMap: collection.Map[String, Int] = buffer1.getMap[String, Int](0)
      val buf2MergedCommonTagMap: collection.Map[String, Int] = buffer2.getMap[String, Int](0)
      val buf2KeywordsSet: collection.Set[String] = buf2MergedCommonTagMap.keySet

      for (keywords <- buf2KeywordsSet) {
        if (buf1MergedCommonTagMap.keySet.contains(keywords))
          buf1MergedCommonTagMap += ((keywords, buf1MergedCommonTagMap.get(keywords).get + buf2MergedCommonTagMap.get(keywords).get))
        else
          buf1MergedCommonTagMap += ((keywords, buf2MergedCommonTagMap.get(keywords).get))
      }

      buffer1.update(0, buf1MergedCommonTagMap)
    }

    override def evaluate(buffer: Row): Any = {
      implicit val formats = Serialization.formats(NoTypeHints)

      val jsonStr: String = write(buffer.getMap[String, Int](0))
      jsonStr
    }
  }

  /**
    * UDAF For Merging Special Tag Likes KeywordsTag
    */
  object mergeSpecialTag extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      StructType(
        StructField("specialTag", StringType) ::
          Nil
      )
    }

    override def bufferSchema: StructType = {
      StructType(
        StructField("mergedSpecialTag", MapType(StringType, IntegerType)) ::
          Nil
      )
    }

    override def dataType: DataType = {
      StringType
    }

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, Map[String, Int]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val keywordsNotArray: String = input.getString(0)

      if (keywordsNotArray != "") {
        var mergedSpecialTagMap: collection.Map[String, Int] = buffer.getMap[String, Int](0)
        val keywordsArray: Array[String] = keywordsNotArray.split("\\|")

        for (keywords <- keywordsArray) {
          if (mergedSpecialTagMap.keySet.contains(keywords))
            mergedSpecialTagMap += ((keywords, mergedSpecialTagMap.get(keywords).get + 1))
          else
            mergedSpecialTagMap += ((keywords, 1))
        }

        buffer.update(0, mergedSpecialTagMap)
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      var buf1MergedSpecialTagMap: collection.Map[String, Int] = buffer1.getMap[String, Int](0)
      val buf2MergedSpecialTagMap: collection.Map[String, Int] = buffer2.getMap[String, Int](0)
      val buf2KeywordsSet: collection.Set[String] = buf2MergedSpecialTagMap.keySet

      for (keywords <- buf2KeywordsSet) {
        if (buf1MergedSpecialTagMap.keySet.contains(keywords))
          buf1MergedSpecialTagMap += ((keywords, buf1MergedSpecialTagMap.get(keywords).get + buf2MergedSpecialTagMap.get(keywords).get))
        else
          buf1MergedSpecialTagMap += ((keywords, buf2MergedSpecialTagMap.get(keywords).get))
      }

      buffer1.update(0, buf1MergedSpecialTagMap)
    }

    override def evaluate(buffer: Row): Any = {
      implicit val formats = Serialization.formats(NoTypeHints)

      val jsonStr: String = write(buffer.getMap[String, Int](0))
      jsonStr
    }
  }

}
