package com.mavric.fifa.spark
import java.util.Properties

object PropertiesLoader {

  val url = getClass.getResource("C:\\Users\\vn022vj\\Downloads\\Fifa_data_Analysis-master\\src\\main\\resource\\postgre.properties")
  val properties: Properties = new Properties()
  //Postgressql Specific Properties

  val driver = properties.getProperty("driver")
  val host =properties.getProperty("host")
  val port = properties.getProperty("port")
 val username = properties.getProperty("username")
  val password = properties.getProperty("password")
  val dbtable = properties.getProperty("dbtable")
  val mode = properties.getProperty("ovmode")
  val url_connect = properties.getProperty("oracleurl")

}
