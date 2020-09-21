import frameless.functions._
import frameless.syntax._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Main extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class BuyObj(sessID:String,
                    date:String,
                    itemID:String,
                   price:String,
                   qty:String)
  val conf = new SparkConf().setMaster("local[*]")
  val spSess = SparkSession
    .builder
    .config(conf)
    .getOrCreate()

  import spSess.implicits._
  val preDF =spSess.read
    .option("header","TRUE")
    .csv("src/data/buys.dat")
    .as[BuyObj]

  val typedDF = TypedDataset.create(preDF)(TypedEncoder[BuyObj])

  typedDF.select(typedDF('sessID)).show().run()
}
