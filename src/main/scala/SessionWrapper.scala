import Main.{BuyObj, ClickObj}
import frameless.{Injection, TypedDataset, TypedEncoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}

trait SessionWrapper {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setMaster("local[*]")
  val spSess = SparkSession
    .builder
    .config(conf)
    .getOrCreate()

  val schema = Encoders.product[BuyObj].schema

  import spSess.implicits._

  val buyDF =spSess.read
    .option("header","TRUE")
    .schema(schema)
    .csv("src/data/buys.dat")
    .as[BuyObj]

  val clicksDF =spSess.read
    .option("header","TRUE")
    .csv("src/data/clicks.dat")
    .as[ClickObj]


  implicit val dateToLongInjection = new Injection[java.sql.Date, Long] {
    def apply(d: java.sql.Date): Long = d.getTime
    def invert(l: Long): java.sql.Date = new java.sql.Date(l)
  }

  val typedBuyDF = TypedDataset.create(buyDF)(TypedEncoder[BuyObj])
  val typedClickDF = TypedDataset.create(clicksDF)(TypedEncoder[ClickObj])
}
