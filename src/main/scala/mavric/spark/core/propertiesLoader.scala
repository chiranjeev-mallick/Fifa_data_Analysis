package mavric.spark.core

import java.util.Properties

import org.apache.log4j.PropertyConfigurator

object propertiesLoader {

  val connectionParam = new Properties
  connectionParam.load(getClass().getResourceAsStream("/postgres.properties"))
  PropertyConfigurator.configure(connectionParam)


  //Postgres Specific properties

  val source = connectionParam.getProperty("source")
  val host = connectionParam.getProperty("host")
  val port = connectionParam.getProperty("port")
  val username = connectionParam.getProperty("username")
  val password = connectionParam.getProperty("password")
  val dbtable = connectionParam.getProperty("dbtable")
  val errormode = connectionParam.getProperty("errormode")
  val ovmode = connectionParam.getProperty("ovmode")
  val ignoremode = connectionParam.getProperty("ignoremode")
  val oracleurl = connectionParam.getProperty("oracleurl")
  val driver = connectionParam.getProperty("driver")

}
