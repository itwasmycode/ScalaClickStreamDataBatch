package main

import frameless.{Injection, TypedDataset, TypedEncoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, regexp_extract, regexp_replace}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import frameless._
import frameless.syntax._
import frameless.functions._

trait SessionWrapper  {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setMaster("local[*]")
  val spSess = SparkSession
    .builder
    .config(conf)
    .getOrCreate()





  def dfExtractor(df: DataFrame) = df.withColumn("date",
    regexp_replace(regexp_extract(col("date"),
      "[0-9]{4}-[0-9]{2}-[0-9]{2}[A-Za-z][0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}",
      0),
      "[A-Za-z]"," "))
    .withColumn("sDate", regexp_extract(col("date"),"[0-9]{4}-[0-9]{2}-[0-9]{2}",0))
    .withColumn("day",date_format(col("date"),"E"))
    .withColumn("hour",hour(col("date")))

  import spSess.implicits._
  val schemaClick = Encoders.product[main.Main.ClickObj].schema
  val schemaBuy = Encoders.product[main.Main.BuyObj].schema
  val buyDF = dfExtractor(spSess.read
    .option("header","TRUE")
    .schema(schemaBuy)
    .csv("src/data/buys.dat")
    .toDF()).as[main.Main.BuyObj]

  val clicksDF = dfExtractor(spSess.read
    .option("header","TRUE")
    .schema(schemaClick)
    .csv("src/data/clicks.dat")
    .toDF()).as[main.Main.ClickObj]




  val typedBuyDF = TypedDataset.create(buyDF)(TypedEncoder[main.Main.BuyObj])
  val typedClickDF = TypedDataset.create(clicksDF)(TypedEncoder[main.Main.ClickObj])

}
