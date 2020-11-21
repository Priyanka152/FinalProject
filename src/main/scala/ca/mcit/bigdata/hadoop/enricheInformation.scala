package ca.mcit.bigdata.hadoop

case class enricheInformation(stationInfo : stationinformation,sysInfo :systeminformation )

object  enricheInformation {
  def formatOutput(stationInfo: stationinformation, sysInfo: systeminformation): String = {
      stationInfo.last_updated + "," +
      stationInfo.ttl + "," +
      stationInfo.capacity + "," +
      stationInfo.eightd_has_key_dispenser + "," +
      stationInfo.electric_bike_surcharge_waiver + "," +
      stationInfo.external_id + "," +
      stationInfo.has_kiosk + "," +
      stationInfo.is_charging + "," +
      stationInfo.lat + "," +
      stationInfo.lon + "," +
      stationInfo.name + "," +
      stationInfo.short_name + "," +
      stationInfo.station_id + "," +
      stationInfo.rental_method + "," +
      sysInfo.email + "," +
      sysInfo.language + "," +
      sysInfo.license_url + "," +
      sysInfo.name + "," +
      sysInfo.operator + "," +
      sysInfo.phone_number + "," +
      sysInfo.purchase_url + "," +
      sysInfo.short_name + "," +
      sysInfo.start_date + "," +
      sysInfo.system_id + "," +
      sysInfo.timezone + "," +
      sysInfo.url


  }
}