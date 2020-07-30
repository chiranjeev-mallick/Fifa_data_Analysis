
  package mavric.spark.core

import org.apache.spark.sql.{DataFrame, SparkSession}
  //import org.apache.spark.sql.DataFrame

  class Postgre_Readwrite {
    def getData(sq: SparkSession): DataFrame = {
      val df = sq.read.format(source = "jdbc")
        .option("url", "URL")
        .option("dbtable", "PLAN_PRODUCT_FLAT")
        .option("user", "PROCESS_PPCM_USER")
        .option("password", "PROCESS_PPCM_USER$123")
        .option("driver", "org.postgresql.Driver")
        .load()
      df
    }
  }
