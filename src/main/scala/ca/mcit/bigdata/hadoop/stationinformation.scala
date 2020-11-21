package ca.mcit.bigdata.hadoop

case class stationinformation(last_updated: Long,
                              ttl :Long,
                              capacity:String,
                              eightd_has_key_dispenser: Boolean ,
                              electric_bike_surcharge_waiver: Boolean ,
                              external_id: String,
                              has_kiosk: Boolean ,
                              is_charging: Boolean ,
                              lat: Double ,
                              lon: Double ,
                              name: String,
                              short_name: String,
                              station_id: String,
                              rental_method:String)
