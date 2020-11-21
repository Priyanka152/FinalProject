package ca.mcit.bigdata.hadoop

class enrichLookup (station :List[systeminformation]) {

  private val lookupTable: Map[Long, systeminformation] = station.map (station => station.ttl -> station).toMap

  def lookup(ttl: Long): systeminformation = lookupTable.getOrElse(ttl, null)

}
