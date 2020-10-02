import frameless.{Injection, TypedDataset, TypedEncoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.functions.{col, date_format, hour, regexp_extract, regexp_replace}
import org.apache.spark.sql.types.{DataType, DateType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

trait SessionWrapper  {
  Logger.getLogger("org").setLevel(Level.ERROR)

  implicit val dateToLongInjection = Injection((_: java.util.Date).getTime(), new java.util.Date((_: Long)))
  implicit val datejvToLongInjection = Injection((_: java.sql.Date).getTime(), new java.sql.Date((_: Long)))

  lazy val conf = new SparkConf().setMaster("local[*]")
  lazy val spSess = SparkSession
    .builder
    .config(conf)
    .getOrCreate()

  private final def dfExtractor(df: DataFrame) = df.withColumn("date",
    regexp_replace(regexp_extract(col("date"),
      "[0-9]{4}-[0-9]{2}-[0-9]{2}[A-Za-z][0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}",
      0),
      "[A-Za-z]"," "))
    .withColumn("sDate", regexp_extract(col("date"),"[0-9]{4}-[0-9]{2}-[0-9]{2}",0))
    .withColumn("day",date_format(col("date"),"E"))
    .withColumn("hour",hour(col("date")))


  import spSess.implicits._
  private lazy val schemaClick = Encoders.product[caseClsStack.ClickObj].schema
  private lazy val schemaBuy = Encoders.product[caseClsStack.BuyObj].schema

  final lazy val buyDF = dfExtractor(spSess.read
    .option("header","TRUE")
    .schema(schemaBuy)
    .csv("src/data/buys.dat")
    ).as[caseClsStack.BuyObj]

  final lazy val clicksDF = dfExtractor(spSess.read
    .option("header","TRUE")
    .schema(schemaClick)
    .csv("src/data/clicks.dat")
    ).as[caseClsStack.ClickObj]



   final lazy val typedBuyDF: TypedDataset[caseClsStack.BuyObj] = TypedDataset.create(buyDF)(TypedEncoder[caseClsStack.BuyObj])
   final lazy val typedClickDF: TypedDataset[caseClsStack.ClickObj] = TypedDataset.create(clicksDF)(TypedEncoder[caseClsStack.ClickObj])

  clicksDF.cache()
  buyDF.cache()
  typedClickDF.cache()
  typedBuyDF.cache()
}
