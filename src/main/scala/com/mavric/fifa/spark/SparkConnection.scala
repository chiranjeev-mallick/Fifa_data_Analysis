package com.mavric.fifa.spark

import org.apache.spark.sql.SparkSession

class SparkConnection {
  def getSparkSession(appName: String): SparkSession = {
    // Creating spark session
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
    spark
  }
}
