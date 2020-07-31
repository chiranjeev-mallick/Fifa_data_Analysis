package com.mavric.fifa.spark
import java.util.Properties

object PropertiesLoader {

  val url = getClass.getResource("C:\\Users\\vn022vj\\Downloads\\Fifa_data_Analysis-master\\src\\main\\resource\\postgre.properties")
  val prop: Properties = new Properties()

  //Postgressql Specific Properties

  val driver = prop.getProperty("driver")
  val port = prop.getProperty("port")
 val username = prop.getProperty("username")
  val password = prop.getProperty("password")
  val dbtable = prop.getProperty("dbtable")
  val mode = prop.getProperty("ovmode")
  val url_connect = prop.getProperty("URL")

}
