package Global_Fields

import org.apache.spark.sql.SaveMode

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/6
  * Time: 15:54
  *
  * @author zbs
  * @version 1.0
  */
object TableName_ODS {
  val FILE_NAME = "/data/2016-10-01_06_p1_invalid.1475274123982.log.FINISH"
  val PATQUET_FILE_PATH = "parquet"
  val ODSTABLE_NAME = "ods_relese"
  val SAVE_MODE = SaveMode.Append
}
