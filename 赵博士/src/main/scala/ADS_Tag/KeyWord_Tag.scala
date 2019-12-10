package ADS_Tag

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/9
  * Time: 21:28
  *
  * @author zbs
  * @version 1.0
  */
object KeyWord_Tag {
  /**
    * @Author zbs
    * @Description:制造关键字标签
    * @Date: 2019/12/9
    * @Param args:
    * @return: scala.collection.immutable.List<scala.Tuple2<java.lang.String,java.lang.Object>>
    */
  def makeTags(args: Seq[Any]): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 获取关键字
    val keyword = args(80).toString
    keyword.split("\\|").filter(word => {
      word.length >= 3 && word.length <= 8
    }).foreach(f => {
      list :+= ("K" + f, 1)
    })
    list.distinct
  }

}
