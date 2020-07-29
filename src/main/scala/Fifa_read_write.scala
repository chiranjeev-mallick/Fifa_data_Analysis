import javafx.beans.binding.When
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import scala.collection.Seq
import java.sql.Timestamp
import  java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.types._
  object Fifa_read_write  extends  App{

    val spark = SparkSession.builder().master("local").appName("fifa").getOrCreate()

    // Reading the data from HDFS Path
    val fifaFiles = spark.read.option("endian", "little")
      .option("encoding", "UTF-8").option("Header", true).csv(args(0))
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
      .option("encoding", "UTF-8").option("Header", true).csv("C:\\Project\\Output\\Fifa\\*\\")
    ExistingFifaData.show()
    fifaFiles
    // Reading the data from fifa HDFS Path
    val NewData = fifaFiles.join(ExistingFifaData,  Seq("rownum"),  "leftanti")

    NewData.show()

    // Writing  the new data to HDFS Path
    NewData.write.option("Header", true).csv(args(1)+"/"+s"$filename")

    spark.close()
}
