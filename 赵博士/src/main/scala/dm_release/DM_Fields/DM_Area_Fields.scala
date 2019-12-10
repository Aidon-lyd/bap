package dm_release.DM_Fields

import scala.collection.mutable.ArrayBuffer

/**
  * Created with IntelliJ IDEA.
  * User: zbs
  * Date: 2019/12/7
  * Time: 14:17
  *
  * @author zbs
  * @version 1.0
  */
object DM_Area_Fields {
  val Areal_ISBID = "isbid"
  val SESSION_RTB = 0
  val PART_BIDDING = "participate_bidding"
  val SUCC_BIDDINGG = "succeed_bidding"
  val DISPLAY = "display"
  val CLICK = "click"
  val AD_CONSUMED = "ad_consumed"
  val AD_COST = "ad_cost"
  val GROUP_FIEDS_PRO="provincename"
  val GROUP_FIEDS_CITY="cityname"
  def condition_Fields = {
    val columns = new ArrayBuffer[String]()

    columns += (s"sessionid")
    //    case when (t.batchid  <>' ' and t.batchid is not null) then 1 else 0 end
    columns += (s"${GROUP_FIEDS_PRO}")
    columns += (s"${GROUP_FIEDS_CITY}")
    columns += (s"case when( iseffective == 1 and isbilling == 1 and isbid ==1 ) then 1 else 0 end ${PART_BIDDING}")
    columns += (s"case when( iseffective == 1 and isbilling == 1 and iswin ==1 and adorderid != 0) then 1 else 0 end ${SUCC_BIDDINGG}")
    columns += (s"case when( requestmode == 2 and iseffective ==1 )then 1 else 0 end ${DISPLAY}")
    columns += (s"case when( requestmode == 3 and iseffective ==1 )then 1 else 0 end ${CLICK}")
    columns += (s"case when( iseffective == 1 and isbilling == 1 and iswin ==1 )then winprice else 0 end ${AD_CONSUMED}")
    columns += (s"case when( iseffective == 1 and isbilling == 1 and iswin ==1 )then adpayment else 0 end ${AD_COST}")
    columns
  }


}
