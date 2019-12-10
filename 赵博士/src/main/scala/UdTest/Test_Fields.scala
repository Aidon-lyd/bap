package UdTest

import scala.collection.mutable.ArrayBuffer

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/6
  * Time: 21:08
  *
  * @author zbs
  * @version 1.0
  */
object Test_Fields {
  def procinces_files = {
    val columns = new ArrayBuffer[String]
    columns += ("sessionid")
    columns += ("provincename")
    columns += ("cityname")
    columns
  }

  val GROUP_FIELDS_PROVIN = "provincename"
  val GROUP_FIELDS_CITY = "cityname"
  val GROUP_FIELDS_SESSIONID="sessionid"
  val GROUP_FIELDS_COUNTDISTIN="ct"
  def group_Fields={
    val columns = new ArrayBuffer[String]
    columns += GROUP_FIELDS_COUNTDISTIN
    columns += GROUP_FIELDS_PROVIN
    columns += GROUP_FIELDS_CITY
    columns
  }
}
