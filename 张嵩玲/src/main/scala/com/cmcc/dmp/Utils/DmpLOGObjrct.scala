package com.cmcc.dmp.Utils

import com.cmcc.dmp.filehand.DmpLOG

/**
  * requirement  字段
  *
  * @author zhangsl
  * @date 2019/12/6 16:22 
  * @version 1.0
  */
object DmpLOGObjrct {
  def getDmpLOG(arr:Array[String]):DmpLOG = {
    return DmpLOG(
      arr(0),
      NumberUtils.intEmpty(arr(1)),
      NumberUtils.intEmpty(arr(2)),
      NumberUtils.intEmpty(arr(3)),
      NumberUtils.intEmpty(arr(4)),
      arr(5),
      arr(6),
      NumberUtils.intEmpty(arr(7)),
      NumberUtils.intEmpty(arr(8)),
      NumberUtils.doubleEmpty(arr(9)),
      NumberUtils.doubleEmpty(arr(10)),
      arr(11),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      NumberUtils.intEmpty(arr(17)),
      arr(18),
      arr(19),
      NumberUtils.intEmpty(arr(20)),
      NumberUtils.intEmpty(arr(21)),
      arr(22),
      arr(23),
      arr(24),
      arr(25),
      NumberUtils.intEmpty(arr(26)),
      arr(27),
      NumberUtils.intEmpty(arr(28)),
      arr(29),
      NumberUtils.intEmpty(arr(30)),
      NumberUtils.intEmpty(arr(31)),
      NumberUtils.intEmpty(arr(32)),
      arr(33),
      NumberUtils.intEmpty(arr(34)),
      NumberUtils.intEmpty(arr(35)),
      NumberUtils.intEmpty(arr(36)),
      arr(37),
      NumberUtils.intEmpty(arr(38)),
      NumberUtils.intEmpty(arr(39)),
      NumberUtils.doubleEmpty(arr(40)),
      NumberUtils.doubleEmpty(arr(41)),
      NumberUtils.intEmpty(arr(42)),
      arr(43),
      NumberUtils.doubleEmpty(arr(44)),
      NumberUtils.doubleEmpty(arr(45)),
      arr(46),
      arr(47),
      arr(48),
      arr(49),
      arr(50),
      arr(51),
      arr(52),
      arr(53),
      arr(54),
      arr(55),
      arr(56),
      NumberUtils.intEmpty(arr(57)),
      NumberUtils.doubleEmpty(arr(58)),
      NumberUtils.intEmpty(arr(59)),
      NumberUtils.intEmpty(arr(60)),
      arr(61),
      arr(62),
      arr(63),
      arr(64),
      arr(65),
      arr(66),
      arr(67),
      arr(68),
      arr(69),
      arr(70),
      arr(71),
      arr(72),
      NumberUtils.intEmpty(arr(73)),
      NumberUtils.doubleEmpty(arr(74)),
      NumberUtils.doubleEmpty(arr(75)),
      NumberUtils.doubleEmpty(arr(76)),
      NumberUtils.doubleEmpty(arr(77)),
      NumberUtils.doubleEmpty(arr(78)),
      arr(79),
      arr(80),
      arr(81),
      arr(82),
      arr(83),
      if(arr(84).isEmpty) 1 else arr(84).trim.toInt
    )
  }
}
