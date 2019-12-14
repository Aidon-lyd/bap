package dmp.traits

import org.apache.spark.sql.SparkSession

trait Processor {
  def processor(spark: SparkSession )
}
