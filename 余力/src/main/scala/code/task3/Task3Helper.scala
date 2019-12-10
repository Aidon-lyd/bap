package code.task3

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

import scala.collection.mutable.ArrayBuffer

object Task3Helper {
  def getColumnFormatOne() = {
    val columns = new ArrayBuffer[Column]()
    columns.+=(expr("SUM(IF(requestmode = 1 and processnode >= 1,1,0))").as("rawRequestNum"))
    columns.+=(expr("SUM(IF(requestmode = 1 and processnode >= 2,1,0))").as("validRequestNum"))
    columns.+=(expr("SUM(IF(requestmode = 1 and processnode = 3,1,0))").as("adRequestNum"))
    columns.+=(expr("SUM(IF(iseffective = 1 and isbilling = 1 and isbid = 1,1,0))").as("participateInBindNum"))
    columns.+=(expr("SUM(IF(iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0,1,0))").as("bindAndWinNum"))
    columns.+=(expr("SUM(IF(requestmode = 2 and iseffective = 1,1,0))").as("explodeNum"))
    columns.+=(expr("SUM(IF(requestmode = 3 and iseffective = 1,1,0))").as("clickNum"))
    columns.+=(expr("SUM(IF(iseffective = 1 and isbilling = 1 and iswin = 1,adpayment/1000,0))").as("adCost"))
    columns.+=(expr("SUM(IF(iseffective = 1 and isbilling = 1 and iswin = 1,winprice/1000,0))").as("adConsume"))
    columns
  }

  def getColumnFormatTwo(colsName:String *) = {
    val columns = new ArrayBuffer[String]()

    for(colName <- colsName){
      columns.+=(colName)
    }

    columns.+=("rawRequestNum")
    columns.+=("validRequestNum")
    columns.+=("adRequestNum")
    columns.+=("participateInBindNum")
    columns.+=("bindAndWinNum")
    columns.+=("bindAndWinNum/participateInBindNum as bindSuccessRate")
    //May Need to use following Expression
    //"if(bindAndWinNum/participateInBindNum is null,0.0,bindAndWinNum/participateInBindNum )as bindSuccessRate",
    columns.+=("explodeNum")
    columns.+=("clickNum")
    columns.+=("clickNum/explodeNum as clickRate")
    columns.+=("adCost")
    columns.+=("adConsume")
    columns
  }
}
