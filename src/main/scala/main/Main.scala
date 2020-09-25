package main

import java.sql.Date

import org.apache.spark.sql.functions.{col, hour, regexp_extract, regexp_replace}
import frameless.functions.nonAggregate.regexpReplace
import frameless.syntax._
import frameless.functions.lit
import frameless.functions.nonAggregate.when
import org.apache.log4j.{Level, Logger}
import frameless._
import frameless.functions._



object Main extends App with SessionWrapper{
  Logger.getLogger("org").setLevel(Level.ERROR)

  /* Implementing cclasses*/
  case class BuyObj(sessID:String,
                    date:String,
                    itemID:Long,
                    price:Long,
                    qty:Int,
                    sDate:String,
                    day: String,
                    hour:Int)

  case class ClickObj(sessID: String,
                      date: String,
                      itemID: Long,
                      pType: Int,
                      sDate:String,
                      day:String,
                      hour:Int)


  val updatedTypeClickedDF = typedClickDF.withColumn[caseClsStack.ClickObjUpdated](
    when(typedClickDF('pType)=!=0,typedClickDF('pType))
                            .otherwise(lit(1)))

  updatedTypeClickedDF.filter(updatedTypeClickedDF('pTypeUpdated)===1).show().run()

}
