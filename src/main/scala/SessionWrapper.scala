import frameless.{TypedDataset, TypedEncoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, date_format, hour, regexp_extract, regexp_replace}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

trait SessionWrapper  {
  /** Creates a person with a given name and age.
   * Main goal of this trait is creating pre-initialized SparkSession, Spark Configuration.
   *  It also provides pre configured TypedDataset for future use by caching them.
   */
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Initializing Conf and Session according to previous config. This case it is locally connected.(8080)
  lazy val conf = new SparkConf().setMaster("local[*]")
  lazy val spSess = SparkSession
    .builder
    .config(conf)
    .getOrCreate()

  private final def dfExtractor(df: DataFrame) =
  /** Extracts base date, hour feature for future uses. This is used by just in this Trait and never will be used.
   * Because of this trait will passes into Singleton objects, there is no reason to share this as a public.
   * So this is private.
   *  @param df An instance of TypedDataFrame[_] includes any format of SQLDate.
   */
    df.withColumn("date",
    regexp_replace(regexp_extract(col("date"),
      "[0-9]{4}-[0-9]{2}-[0-9]{2}[A-Za-z][0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}",
      0),
      "[A-Za-z]"," "))
    .withColumn("sDate", regexp_extract(col("date"),"[0-9]{4}-[0-9]{2}-[0-9]{2}",0))
    .withColumn("day",date_format(col("date"),"E"))
    .withColumn("hour",hour(col("date")))

  // Create encoders for two base TypedDatasets. If there is no schema, cannot be inferred by case classes.
  import spSess.implicits._
  private lazy val schemaClick = Encoders.product[caseClsStack.ClickObj].schema
  private lazy val schemaBuy = Encoders.product[caseClsStack.BuyObj].schema

 // This dataset includes Checkout-Success Page Transaction corresponding to sessionIDs if it exists.
  final lazy val buyDF = dfExtractor(spSess.read
    .option("header","TRUE")
    .schema(schemaBuy)
    .csv("src/data/buys.dat")
    ).as[caseClsStack.BuyObj]

  // This dataset just a clickstream log made by user. It will be parsed to predict session will be resulted with success.
  final lazy val clicksDF = dfExtractor(spSess.read
    .option("header","TRUE")
    .schema(schemaClick)
    .csv("src/data/clicks.dat")
    ).as[caseClsStack.ClickObj]


  // Creating TypedDataset for Frameless is done by TypedDataSet.create command. It needs a spark session.
   final lazy val typedBuyDF: TypedDataset[caseClsStack.BuyObj] = TypedDataset.create(buyDF)(TypedEncoder[caseClsStack.BuyObj])
   final lazy val typedClickDF: TypedDataset[caseClsStack.ClickObj] = TypedDataset.create(clicksDF)(TypedEncoder[caseClsStack.ClickObj])

  /*
  * Since these are going to be injected to another objects, need to be cached in order to reduce compile time.
  * Note that Frameless has longer compile time when it is compared to Spark DataSets.
  * If your goal is analysis, use Spark, but there is no guarantee that Spark will provide TypeSafetiness of analysis.
   */
  clicksDF.cache()
  buyDF.cache()
  typedClickDF.cache()
  typedBuyDF.cache()
}
