package ca.mcit.bigdata.hadoop

import org.apache.spark.sql.functions.{concat_ws, explode}
import org.apache.spark.sql.{Encoders, SparkSession}
import scala.collection.JavaConversions.collectionAsScalaIterable

object Main extends hiveClient with App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("spark")
    .getOrCreate()
  val sc = spark.sparkContext

  /*station_information*/
  val df = spark.read.json("/home/bd-user/Documents/station_information.json")

  import spark.implicits._
  val df1 = df.select($"last_updated", $"ttl", explode($"data.stations"))
    .select("last_updated", "ttl", "col.*")
    .withColumn("rental_method", concat_ws("|", $"rental_methods"))
    .drop("rental_methods")

  val StaionInfocsvPath = "/home/bd-user/Documents/csv/station_information.csv"
  df1.write.mode("overwrite")
    .format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .csv(StaionInfocsvPath)
  df1.show(5)
  df1.printSchema()

  /*system_information*/
  val df2 = spark.read.json("/home/bd-user/Documents/system_information.json")
  val df3 = df2.select("last_updated", "ttl", "data.*")
  val systemInfocsvPath = "/home/bd-user/Documents/csv/system_information.csv"
  df3.write.mode("overwrite")
    .format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .csv(systemInfocsvPath)
  df3.printSchema()
  df3.show(5)

  /*Enrichment*/
  val stationschema = Encoders.product[stationinformation].schema
  val df4: List[stationinformation] = spark.read.schema(stationschema)
    .csv(StaionInfocsvPath).as[stationinformation].collectAsList().toList

  val systemschema = Encoders.product[systeminformation].schema
  val df5: List[systeminformation] = spark.read.schema(systemschema)
    .csv(systemInfocsvPath).as[systeminformation].collectAsList().toList

  val stationLookup = new enrichLookup(df5)
  val enrichedStation: List[enricheInformation] = df4.map(system => enricheInformation(system, stationLookup.lookup(system.ttl)))
  val df6: Unit = spark.createDataFrame(enrichedStation)
    .select("stationInfo.station_id", "stationInfo.short_name", "stationInfo.name", "stationInfo.has_kiosk", "stationInfo.capacity",
      "stationInfo.eightd_has_key_dispenser", "stationInfo.external_id", "stationInfo.rental_method", "stationInfo.electric_bike_surcharge_waiver",
      "stationInfo.lat", "stationInfo.lon","stationInfo.is_charging", "sysInfo.system_id", "sysInfo.email", "sysInfo.start_date", "sysInfo.language",
      "sysInfo.phone_number", "sysInfo.timezone", "sysInfo.url").toDF()
    .repartition(1).write.mode("overwrite")
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("/home/bd-user/Documents/csv/output.csv")
  println(df6)

}

