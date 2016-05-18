import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Months
import org.joda.time.format.ISODateTimeFormat
import com.github.nscala_time.time.Imports._

import scala.collection.immutable.SortedMap
import scala.io.Source
import scala.util.{Failure, Success, Try}

object Test3 {

  val sc = {
    val conf = new SparkConf()
        .setAppName("Test3")
        .setMaster("local[*]")
    SparkContext.getOrCreate(conf)
  }

  case class Booking(airport: String, pax: Integer)

  def main (args: Array[String]) {

    val csvPath = getClass.getResource("/searches.csv").getPath

    val headerLine = Source.fromFile(csvPath)
        .getLines()
        .take(1)
        .toList
        .head

    val colNames =
      headerLine.split("""\^""", -1)
          .map(_.trim)
          .zipWithIndex
          .toMap

    println(s"Colnames: ${colNames.keys}")

    val searches =
      sc.textFile(csvPath)
        .filter(_ != headerLine)
        .map{ record =>
          Try{
            val fields = record.split("""\^""", -1).map(_.trim)
            val dateFormat = ISODateTimeFormat.date()
            dateFormat.parseLocalDate(fields(colNames("Date"))) -> fields(colNames("Destination"))
          }.recoverWith{ case cause =>
            Failure(new IllegalArgumentException(s"Parsing failed with line: $record", cause))
          }
        }

    searches.collect{case Failure(cause) => cause}.collect().foreach(println)

    val keyAirports = Set("MAD", "AGP", "BCN")

    val monthlySearches =
      searches.collect{ case Success((date, dest)) => (date.withDayOfMonth(1), dest) -> date }
              .filter{ case ((_, dest), _) => keyAirports.contains(dest) }
              .aggregateByKey(0)((total, searchDate) => total + 1, _+_)
              .map{ case ((month, dest), nbSearches) => dest -> (month, nbSearches)}
              .groupByKey()
              .collectAsMap()


    val airportCSVPath = getClass.getResource("/airport_geo_names_only.csv").getPath
    val airports =
      sc.textFile(airportCSVPath)
          .map{ record =>
            val fields = record.split("""\^""", -1).take(2).map(_.trim)
            fields(0) -> fields(1)
          }
          .filter{ case (code, name) => keyAirports.contains(code)}
          .collectAsMap()



    monthlySearches.foreach{ case (airport_code, monthly_searches) =>
      println(s"${airports(airport_code)}: ${monthly_searches.toList}")
    }

    val searchesByDest = monthlySearches.mapValues(SortedMap.empty[LocalDate, Int] ++ _)
    val firstMonth = searchesByDest.flatMap{case (dest, srch) => srch.keys}.min

    import breeze.plot._

    val searchFig = Figure()
    val p = searchFig.subplot(0)
    searchesByDest.foreach{ case (dest, monthSearches) =>
      p += plot(
        x = monthSearches.keys.toIndexedSeq.map(Months.monthsBetween(firstMonth,_).getMonths),
        y = monthSearches.values.toIndexedSeq,
        name = dest
      )
    }
    searchFig.refresh()

    p.title = "Monthly searches"
    p.xlabel = "Months"
    p.ylabel = "Nb searches"
    p.legend = true

    searchFig.refresh()

  }
}
