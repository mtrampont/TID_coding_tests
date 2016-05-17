import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test1 {

  val sc = {
    val conf = new SparkConf()
        .setAppName("Test1")
        .setMaster("local[*]")
    SparkContext.getOrCreate(conf)
  }

  def main (args: Array[String]) {

    val bookingRecords = sc.textFile(getClass.getResource("/bookings.csv").getPath)
    val searchRecords = sc.textFile(getClass.getResource("/searches.csv").getPath)

    println(s"NbBookings: ${bookingRecords.count()}")
    println(s"NbSearches: ${searchRecords.count()}")
  }
}
