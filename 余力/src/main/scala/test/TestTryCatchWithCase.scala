package test

object TestTryCatchWithCase {
  def main(args: Array[String]): Unit = {
    println(testTrtCatchWithCase())
  }

  def testTrtCatchWithCase()={
    try {
      "a".toInt
    } catch {
      case _ :Exception => 1
    }
  }
}
