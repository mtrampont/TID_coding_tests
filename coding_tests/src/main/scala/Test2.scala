import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap
import scala.io.Source
import scala.util.{Try, Failure, Success}

object Test2 {

  val sc = {
    val conf = new SparkConf()
        .setAppName("Test2")
        .setMaster("local[*]")
    SparkContext.getOrCreate(conf)
  }

  case class Booking(airport: String, pax: Integer)

  def main (args: Array[String]) {

    val csvPath = getClass.getResource("/bookings.csv").getPath

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

    val bookings =
      sc.textFile(csvPath)
        .filter(_ != headerLine)
        .map{ record =>
          Try{
            val fields = record.split("""\^""", -1).map(_.trim)
            (fields(colNames("arr_port")), fields(colNames("pax")).toInt)
          }.recoverWith{ case cause =>
            Failure(new IllegalArgumentException(s"Parsing failed with line: $record", cause))
          }
        }

    bookings.foreach{
      case Failure(cause) => println(cause)
      case _ =>
    }

    val top10 =
      bookings.collect{ case Success(bkg) => bkg}
              .aggregateByKey(0)(_+_, _+_)
              .top(10)(Ordering.by[(String, Int),Int](_._2))

    top10.foreach{ case (airport, passengers) =>
      println(s"$airport: $passengers")
    }

    val top10Codes = top10.map(_._1).toSet

    val airportCSVPath = getClass.getResource("/airport_geo_names_only.csv").getPath
    val airports =
      sc.textFile(airportCSVPath)
          .map{ record =>
            val fields = record.split("""\^""", -1).take(2).map(_.trim)
            fields(0) -> fields(1)
          }
          .filter{ case (code, name) => top10Codes.contains(code)}
          .collectAsMap()

    top10.foreach{ case (airport_code, passengers) =>
      println(s"${airports(airport_code)}: $passengers")
    }

  }
}
