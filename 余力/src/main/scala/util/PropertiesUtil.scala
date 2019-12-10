package util

import java.util.Properties

object PropertiesUtil {
  def getPropertiesOfJDBCMySQL()={
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    properties
  }
}
