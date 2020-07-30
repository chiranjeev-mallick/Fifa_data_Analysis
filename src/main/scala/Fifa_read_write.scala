import scala.collection.Seq
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import com.mavric.fifa.spark.SparkConnection

  object Fifa_read_write  extends  App{
    val sc = new SparkConnection
    val spark =sc.getSparkSession("Fifa_Data_readWrite")


    // Reading the data from HDFS Path
    val fifaFiles = spark.read.option("endian", "little")
      .option("encoding", "UTF-8").option("Header", "true").csv(args(0))
      .withColumnRenamed("_c0", "rownum")
    def getCurrentTimestamp: Timestamp = {
      val today: java.util.Date = Calendar.getInstance.getTime
      val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val now: String = timeFormat.format(today)
      val currentDate = java.sql.Timestamp.valueOf(now)
      currentDate
    }
    val now = getCurrentTimestamp
    val timestampFormatted =  new SimpleDateFormat("yyyyMMddHHmmss")
    val timestamp = timestampFormatted.format(now)
    val filename = "fifa"+timestamp
    val ExistingFifaData = spark.read.option("endian", "little")
      .option("encoding", "UTF-8").option("Header", "true").csv("C:\\Project\\Output\\Fifa\\*\\")
    ExistingFifaData.show()
    fifaFiles
    // Reading the data from fifa HDFS Path
    val NewData = fifaFiles.join(ExistingFifaData,  Seq("rownum"),  "leftanti")

    NewData.show()
    // Writing  the new data to HDFS Path
   if(NewData.count() ==  1){

     NewData.write.option("Header", "true").csv(args(1) + "/" + s"$filename")
   }
    else {
     print("Now new record fetch")

   }
    spark.close()

  }
