package ca.mcit.bigdata.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.sql.{Connection, DriverManager, Statement}


class hiveClient {

  var conf = new Configuration()
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  var fs: FileSystem = FileSystem.get(conf)
  val path = new Path("/user/fall2019/priyanka/spring2")
  if (fs.exists(path)) {
    println("deleting file : " + path)
    fs.delete(path, true)
  }
  else {
    println("File/Directory" + path + " does not exist")
  }
  fs.mkdirs(new Path("/user/fall2019/priyanka/spring2"))
  val srcPath = new Path("/home/bd-user/Documents/csv/output.csv")
  val desPath = new Path("/user/fall2019/priyanka/spring2")
  fs.copyFromLocalFile(srcPath, desPath)

  /*Hive External table*/
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000/fall2019_priyanka;user=priyanka;password=priyanka")
  val stmt: Statement = connection.createStatement()
  val tableName: String = "enrich_stationinformation"
  stmt.execute("DROP TABLE IF EXISTS  s19909_bixi_feed_priyanka.enrich_station_information")

  stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS " + "s19909_bixi_feed_priyanka.enrich_stationinformation ( " +
    "station_id STRING ," +
    "short_name String ," +
    "name String ," +
    "has_kiosk Boolean ," +
    "capacity String ," +
    "eightd_has_key_dispenser Boolean ," +
    "external_id String ," +
    "rental_method String," +
    "electric_bike_surcharge_waiver Boolean ," +
    "lat Double," +
    "lon Double," +
    "is_charging String ," +
    "system_id STRING ," +
    "email String ," +
    "start_date String ," +
    "language String ," +
    "phone_number String ," +
    "timezone String ," +
    "url String ) " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/fall2019/priyanka/spring2/output.csv' ")
}
