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

object Fifa_data {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("Fifa_data")
      .getOrCreate()

    import spark.implicits._

    val Read_FifaData = spark.read
      .option("endian", "little") //hadnle symbolic charcter
      .option("header", true)
      .option("inferschema", true)
      .option("encoding", "UTF-8") //handle special charcter
      .csv("C:/Users/vn022vj/Downloads/Fifa_data/target/data.csv")
      .withColumnRenamed("_c0", "rownum")



    //# Selective Columns for Analysis
    val Fifa_Clean_Data = Read_FifaData.withColumn("Id", when(col("Id").isNull, 0).otherwise(col("Id")))
      .withColumn("Name", when(col("Name").isNull, "Unknown").otherwise(col("Name")))
      .withColumn("Age", when(col("Age").isNull, 0).otherwise(col("Age")))
      .withColumn("Nationality", when(col("Nationality").isNull, "Unknown").otherwise(col("Nationality")))
      .withColumn("Overall", when(col("Overall").isNull, 0).otherwise(col("Overall")))
      .withColumn("Potential", when(col("Potential").isNull, 0).otherwise(col("Potential")))
      .withColumn("Club", when(col("Club").isNull, "Unknown").otherwise(col("Club")))
      .withColumn("Value", when(col("Value").isNull, 0).otherwise(col("Value")))
      .withColumn("Wage", when(col("Wage").isNull, 0).otherwise(col("Wage")))
      .withColumnRenamed("Preferred Foot", "Preferred_Foot")
      .withColumn("Preferred_Foot", when(col("Preferred_Foot").isNull, "Unknown").otherwise(col("Preferred_Foot")))
      .withColumn("Position", when(col("Position").isNull, "Unknown").otherwise(col("Position")))
      .withColumn("Crossing", when(col("Crossing").isNull, 0).otherwise(col("Crossing")))
      .withColumn("GKDiving", when(col("GKDiving").isNull, 0).otherwise(col("GKDiving")))
      .withColumn("GKHandling", when(col("GKHandling").isNull, 0).otherwise(col("GKHandling")))
      .withColumn("GKKicking", when(col("GKKicking").isNull, 0).otherwise(col("GKKicking")))
      .withColumn("GKPositioning", when(col("GKPositioning").isNull, 0).otherwise(col("GKPositioning")))
      .withColumn("GKReflexes", when(col("GKReflexes").isNull, 0).otherwise(col("GKReflexes")))

    Fifa_Clean_Data.printSchema()


    //# cleaning Wage and Value columns from currency to normal numbers
    val wagedf = Read_FifaData.withColumn("wages", (regexp_replace(regexp_replace($"wage", "€", ""), "K", "").cast("Int") * 1000).cast("Decimal(10,2)")).drop("Wage")
    val valuedf = Read_FifaData.withColumn("Value", when(col("Value").contains("M"), (regexp_replace(regexp_replace($"Value", "€", ""), "M", "").cast("float") * 1000000).cast("Decimal(20,2)"))
      .otherwise(when(col("Value").contains("K"), (regexp_replace(regexp_replace($"Value", "€", ""), "K", "000")))))


    //#Getting top club having leftfooted midfielder under age 30
    Fifa_Clean_Data.filter($"Position".isin("RWM", "RM", "RCM", "CM", "CAM", "CDM", "LCM", "LM", "LWM") && $"Preferred Foot" === "Left" && $"Age" < "30")
      .withColumn("leftmidfielder_count", count("Club").over(Window.partitionBy("Club")))
      .select("Club", "leftmidfielder_count")
      .dropDuplicates()
      .orderBy(desc("leftmidfielder_count")).show(false)

    //strongest team by overall rating for a 4-4-2 formation
    Fifa_Clean_Data.filter($"Position".isin("GK", "RB", "CB", "RCB", "CB", "LCB", "LB", "RM", "RWM", "LCM", "CM", "RCM", "CM", "LM", "LWM", "RF", "CF", "LF", "ST"))
      .withColumn("Avg_Rating", avg("Overall").over(Window.partitionBy("Club")))
      .select("Club", "Avg_Rating").dropDuplicates()
      .withColumn("Rank", row_number().over(Window.orderBy(desc("Avg_Rating"))))


    //#expensive squad value in the world
    val Expensive_squadValue = valuedf.withColumn("squad_value", sum(("Value")).over(Window.partitionBy("Nationality")))
      .select("Nationality", "squad_value").dropDuplicates().withColumn("Rank", row_number().over(Window.orderBy(desc("squad_value")))).where("Rank == 1")
    val squad_wages = wagedf.withColumn("Squad_wage", sum("wages").over(Window.partitionBy(("Nationality"))))
      .select("Nationality", "Squad_wage").dropDuplicates().withColumn("Rank", row_number().over(Window.orderBy(desc("Squad_wage")))).where("Rank == 1")
    val compare_nation = if (Expensive_squadValue.join(squad_wages, Seq("Nationality"), "Inner").count() == 1) {
      print("Strongest squad value having Highest wages")
    } else {
      print("Strongest squad value does not have Highest wages")
    }


   // Print("Does Expensive Squad in word have highest wages" +))

    //# Position pays highest wage in Average
  val Highest_wagebyPositon=wagedf.withColumn("Avg_wage",avg("wages").over(Window.partitionBy("Position")))
                                  .select("Position","Avg_wage")
                                  .dropDuplicates()
    .withColumn("Rank",row_number().over(Window.orderBy(desc("Avg_wage")))).show(false)



    //#TO find out Avg of attribute of player for Goalkeeper Rating
    val Avg_GoalKeepers = Fifa_Clean_Data.filter($"Position".isin("GK") && $"Overall" > 80)
      .select(avg("Crossing").alias("avg_Crossing")
        , avg("Finishing").alias("avg_Finishing")
        , avg("HeadingAccuracy").alias("avg_HeadingAccuracy")
        , avg("ShortPassing").alias("avg_ShortPassing")
        , avg("Volleys").alias("avg_Volleys")
        , avg("Dribbling").alias("avg_Dribbling")
        , avg("Curve").alias("avg_Curve")
        , avg("FKAccuracy").alias("avg_FKAccuracy")
        , avg("LongPassing").alias("avg_LongPassing")
        , avg("BallControl").alias("avg_BallControl")
        , avg("Acceleration").alias("avg_Acceleration")
        , avg("SprintSpeed").alias("avg_SprintSpeed")
        , avg("Agility").alias("avg_Agility")
        , avg("Reactions").alias("avg_Reactions")
        , avg("Balance").alias("avg_Balance")
        , avg("ShotPower").alias("avg_ShotPower")
        , avg("Jumping").alias("avg_Jumping")
        , avg("Stamina").alias("avg_Stamina")
        , avg("Strength").alias("avg_Strength")
        , avg("LongShots").alias("avg_LongShots")
        , avg("Aggression").alias("avg_Aggression")
        , avg("Interceptions").alias("avg_Interceptions")
        , avg("Positioning").alias("avg_Positioning")
        , avg("Vision").alias("avg_Vision")
        , avg("Penalties").alias("avg_Penalties")
        , avg("Composure").alias("avg_Composure")
        , avg("Marking").alias("avg_Marking")
        , avg("StandingTackle").alias("avg_StandingTackle")
         ,avg("SlidingTackle").alias("avg_SlidingTackle")
      ,avg("GKDiving").alias("avg_GKDiving")
      ,avg("GKHandling").alias("avg_GKHandling")
      ,avg("GKKicking").alias("avg_GKKicking")
      ,avg("GKPositioning").alias("avg_GKPositioning")
      ,avg("GKReflexes").alias("GKReflexes1")).show()

    //# checking for 5 attributes to be consider for Good Stricker by taking avg of attributes

    val Avg_Stricker= Fifa_Clean_Data.filter($"Position".isin("ST") && $"Overall" > 80)
      .select(avg("Crossing").alias("avg_Crossing")
        , avg("Finishing").alias("avg_Finishing")
        , avg("HeadingAccuracy").alias("avg_HeadingAccuracy")
        , avg("ShortPassing").alias("avg_ShortPassing")
        , avg("Volleys").alias("avg_Volleys")
        , avg("Dribbling").alias("avg_Dribbling")
        , avg("Curve").alias("avg_Curve")
        , avg("FKAccuracy").alias("avg_FKAccuracy")
        , avg("LongPassing").alias("avg_LongPassing")
        , avg("BallControl").alias("avg_BallControl")
        , avg("Acceleration").alias("avg_Acceleration")
        , avg("SprintSpeed").alias("avg_SprintSpeed")
        , avg("Agility").alias("avg_Agility")
        , avg("Reactions").alias("avg_Reactions")
        , avg("Balance").alias("avg_Balance")
        , avg("ShotPower").alias("avg_ShotPower")
        , avg("Jumping").alias("avg_Jumping")
        , avg("Stamina").alias("avg_Stamina")
        , avg("Strength").alias("avg_Strength")
        , avg("LongShots").alias("avg_LongShots")
        , avg("Aggression").alias("avg_Aggression")
        , avg("Interceptions").alias("avg_Interceptions")
        , avg("Positioning").alias("avg_Positioning")
        , avg("Vision").alias("avg_Vision")
        , avg("Penalties").alias("avg_Penalties")
        , avg("Composure").alias("avg_Composure")
        , avg("Marking").alias("avg_Marking")
        , avg("StandingTackle").alias("avg_StandingTackle")
          ,avg("GKDiving").alias("avg_GKDiving")
          ,avg("GKHandling").alias("avg_GKHandling")
          ,avg("GKKicking").alias("avg_GKKicking")
          ,avg("GKPositioning").alias("avg_GKPositioning")
          ,avg("GKReflexes").alias("GKReflexes")).show()

      //#Saving Dataframe in Postgre with relevent attribute
      val Postgre_file=Fifa_Clean_Data.select($"Overall".cast("Integer").alias("Overall_Rating"),
                              $"Position",
      $"Nationality".alias("Country"),
      $"Name",
      $"Club".alias("Club Name"),
      $"Wage".cast("Decimal(20,5)"),
      $"Value".cast("Decimal(20,2)"),
      $"joined".cast("Date"),
      $"Age".cast("Integer")).printSchema()
//Write data into Postgre:-(Commnet i dont postgre connection)
  //    Postgre_file.write.option('driver', 'org.postgresql.Driver'').jdbc(url_connect, table, mode, properties)
  }

}