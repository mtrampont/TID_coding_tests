import org.apache.spark.{SparkConf, SparkContext}

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

    //  val sqlContext = new SQLContext(sc)

    //    import org.apache.spark.sql.types.{StringType, StructField, IntegerType, StructType}
//    import org.apache.spark.sql.SQLContext
    //    val bookingSchema = StructType(Array(
    //      StructField("arr_port", StringType),
    //      StructField("pax", IntegerType)
    //    ))

    //    val csvReader = sqlContext.read
    //        .format("com.databricks.spark.csv")
    //        .option("header", "true")
    //        .option("delimiter", "^")
    //
    //    val bookings =
    //      csvReader.load(getClass.getResource("/bookings_extract.csv").getPath)

    //    val searches =
//      sqlContext.read
//          .format("com.databricks.spark.csv")
//          .option("header", "true")
//          .option("delimiter", "^")
//          .load(getClass.getResource("/searches.csv").getPath)

//    bookings.printSchema()
//    bookings.show(10)
    //println(s"NbBookings: ${bookings.count()}")
    //println(s"NbSearches: ${searches.count()}")
  }
}
